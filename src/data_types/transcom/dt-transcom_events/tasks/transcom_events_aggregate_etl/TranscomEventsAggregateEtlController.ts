import { mkdirSync, rmSync, existsSync } from "fs";
import { join } from "path";

import dedent from "dedent";
import pgFormat from "pg-format";

import dama_db from "data_manager/dama_db";
import dama_meta from "data_manager/meta";
import dama_events from "data_manager/events";

import { getLoggerForContext } from "data_manager/logger";
import { getEtlContextId } from "data_manager/contexts";

import {
  downloadTranscomEvents,
  makeTranscomEventIdIteratorFromApiScrapeDirectory,
} from "../transcom_events";

import {
  downloadTranscomEventsExpanded,
  loadApiScrapeDirectoryIntoDatabase,
} from "../transcom_events_expanded";

import {
  getTimestamp,
  getTranscomRequestFormattedTimestamp,
} from "../utils/dates";

import TranscomEventsToAdminGeographiesController from "./TranscomEventsToAdminGeographiesController";
import TranscomEventsToConflationMapController from "./TranscomEventsToConflationMapController";

const logger = getLoggerForContext();

const etlBaseDir = join(__dirname, "../../../", "etl-work-dirs");

export default class TranscomEventsAggregateEtlControler {
  protected _etlStart: Date;
  protected _etlWorkDir: string;
  protected etlEnd: Date;
  protected etlTranscomEventsDir!: string;
  protected etlTranscomEventsExpandedDir!: string;
  protected elt_context_id: number;

  constructor(
    protected readonly pgEnv: PgEnv,
    protected eventsStartTime: Date | null = null,
    protected eventsEndTime: Date | null = null
  ) {
    this.elt_context_id = <number>getEtlContextId();
  }

  // SIDE EFFECT WARNING: etlStart initialized on first get
  //   (Facilitates extending this class.)
  protected get etlStart() {
    if (this._etlStart) {
      return this._etlStart;
    }

    this._etlStart = new Date();

    return this._etlStart;
  }

  // TODO: Get the etl_work_dir from the context and replace this with a lock-file or something.
  // SIDE EFFECT WARNING: etlWorkDir initialized on first get
  //   (Facilitates extending this class.)
  protected get etlWorkDir() {
    if (this._etlWorkDir) {
      return this._etlWorkDir;
    }

    const etlStartTs = getTimestamp(this.etlStart);

    const workDirName = `transcom_events_aggregate_etl.${etlStartTs}`;

    // @ts-ignore
    this._etlWorkDir = join(etlBaseDir, workDirName);

    if (existsSync(this.etlWorkDir)) {
      throw new Error("TRANSCOM Events Aggregate ETL running concurrently?");
    }

    mkdirSync(this.etlWorkDir, { recursive: true });

    this.etlTranscomEventsDir = join(this.etlWorkDir, "transcom-events");
    mkdirSync(this.etlTranscomEventsDir);

    this.etlTranscomEventsExpandedDir = join(
      this.etlWorkDir,
      "transcom-events-expanded"
    );

    mkdirSync(this.etlTranscomEventsExpandedDir);

    return this._etlWorkDir;
  }

  async getTranscomEventsLatestStartDateTimeFromDatabase() {
    const {
      rows: [{ latest_start_time }],
    } = await dama_db.query(`
      SELECT
          GREATEST (
            '2010-01-01T00:00'::TIMESTAMP, -- If table is empty, start with this timetamp.
            MAX(start_date_time)
          ) AS latest_start_time
        FROM _transcom_admin.transcom_events_expanded
    `);

    return latest_start_time;
  }

  protected async initializeRequestedTranscomEventsDateExtent() {
    if (!this.eventsStartTime) {
      const latestUpdate =
        await this.getTranscomEventsLatestStartDateTimeFromDatabase();

      this.eventsStartTime = latestUpdate;
    }

    if (!this.eventsEndTime) {
      this.eventsEndTime = this.etlStart;
    }
  }

  protected get initialEtlControlMetadata() {
    return {
      etlTask: "INSERT_TRANSCOM_EVENTS",
      etlStart: this.etlStart,
      eventsStartTime: this.eventsStartTime,
      eventsEndTime: this.eventsEndTime,
      etlWorkDir: this.etlWorkDir,
      status: "IN_PROGRESS",
      subprocesses: [],
    };
  }

  // TODO:  This should be replaced with dispatching DamaEvents.
  //        Then, implement idempotency using the events.
  protected async updateDbControlTableEntry(path: string[], value: any) {
    await dama_db.query(
      `
        UPDATE _transcom_admin.etl_control
          SET metadata = jsonb_set(metadata, $1, $2, true)
          WHERE ( id = $3 )
      `,
      [path, JSON.stringify(value), this.etlControlId]
    );
  }

  protected async closeDbControlTableEntry() {
    await dama_db.query(
      `
        UPDATE _transcom_admin.etl_control
          SET end_timestamp = $1
          WHERE ( id = $2 )
      `,
      [this.etlEnd, this.etlControlId]
    );
  }

  protected async downloadTranscomEvents() {
    await dama_events.dispatch({
      type: ":START_TRANSCOM_EVENTS_DOWNLOAD",
      // @ts-ignore
      payload: {
        events_start_time: this.eventsStartTime,
        events_end_time: this.eventsEndTime,
      },
    });

    await downloadTranscomEvents(
      getTranscomRequestFormattedTimestamp(this.eventsStartTime),
      getTranscomRequestFormattedTimestamp(this.eventsEndTime),
      this.etlTranscomEventsDir
    );

    await dama_events.dispatch({
      type: ":TRANSCOM_EVENTS_DOWNLOAD_DONE",
    });
  }

  protected get transcomEventIdAsyncIteratorFromApiScrapeDirectory() {
    return makeTranscomEventIdIteratorFromApiScrapeDirectory(
      this.etlTranscomEventsDir
    );
  }

  protected async downloadTranscomEventsExpanded() {
    // TODO:  Pass a schema-analysis object to the
    //        If a type changed that wouldn't break downstream code,
    //          update the table definition.
    //        If a type changed that would break downstream code,
    //          notify via Slack and HALT.
    logger.debug("downloadTranscomEventsExpanded start");

    await dama_events.dispatch({
      type: ":START_TRANSCOM_EVENTS_EXPANDED_DOWNLOAD",
    });

    logger.silly("dispatched :START_TRANSCOM_EVENTS_EXPANDED_DOWNLOAD");

    await downloadTranscomEventsExpanded(
      this.transcomEventIdAsyncIteratorFromApiScrapeDirectory,
      this.etlTranscomEventsExpandedDir
    );

    logger.silly("TranscomEventsExpanded downloaded");

    await dama_events.dispatch({
      type: ":TRANSCOM_EVENTS_EXPANDED_DOWNLOAD_DONE",
    });

    logger.silly("dispatched :TRANSCOM_EVENTS_EXPANDED_DOWNLOAD_DONE");

    logger.debug("downloadTranscomEventsExpanded done");
  }

  protected get transcom_events_expanded_staging_table() {
    return {
      table_schema: "staging",
      table_name: `transcom_events_etl_ctx_${this.elt_context_id}`,
    };
  }

  protected async createTranscomEventsExpandedStagingTable() {
    // TODO: IDEMPOTENCY
    const { table_schema, table_name } =
      this.transcom_events_expanded_staging_table;

    const sql = dedent(
      pgFormat(
        `
          CREATE SCHEMA IF NOT EXISTS staging ;

          CREATE TABLE IF NOT EXISTS staging.%I (
            LIKE _transcom_admin.transcom_events_expanded
              INCLUDING DEFAULTS      -- necessary for _created_timestamp & _modified_timestamp
              EXCLUDING CONSTRAINTS   -- because scrapes may violate PrimaryKey CONSTRAINT
          ) ;
        `,
        table_schema,
        table_schema,
        table_name
      )
    );

    logger.silly(sql);

    await dama_db.query(sql);

    await dama_events.dispatch({
      type: ":CREATED_TRANSCOM_EVENTS_EXPANDED_STAGING_TABLE",
      // @ts-ignore
      payload: {
        table_schema,
        table_name,
      },
    });
  }

  protected async dropTranscomEventsExpandedStagingTable() {
    // TODO: IDEMPOTENCY

    const { table_schema, table_name } =
      this.transcom_events_expanded_staging_table;

    const sql = pgFormat(
      "DROP TABLE IF EXIST %I.%I ;",
      table_schema,
      table_name
    );

    await dama_db.query(sql);
  }

  protected async stageTranscomEventsExpanded() {
    const start_timestamp = new Date();
    await this.updateDbControlTableEntry(["transcom_events_expanded_staging"], {
      start_timestamp,
    });

    await this.createTranscomEventsExpandedStagingTable();

    const { table_schema, table_name } =
      this.transcom_events_expanded_staging_table;

    await loadApiScrapeDirectoryIntoDatabase(
      this.etlTranscomEventsExpandedDir,
      table_schema,
      table_name
    );

    await this.updateDbControlTableEntry(["transcom_events_expanded_staging"], {
      start_timestamp,
      end_timestamp: new Date(),
    });
  }

  // FIXME: This should create a new DamaView
  protected async setDbControlTableEtlSummary() {
    const sql = pgFormat(
      `
      SELECT
          COUNT(1) AS num_events,
          MIN(start_date_time) AS min_event_start_date_time,
          MAX(start_date_time) AS max_event_start_date_time
        FROM _transcom_admin.%I
    `,
      this.transcomEventsExpandedStagingTableName
    );

    const {
      rows: [
        { num_events, min_event_start_date_time, max_event_start_date_time },
      ],
    } = await dama_db.query(sql);

    await this.updateDbControlTableEntry(["summary"], {
      num_events,
      event_start_times_extent: [
        min_event_start_date_time,
        max_event_start_date_time,
      ],
    });
  }

  protected async doTranscomEventsQA() {
    // TODO: Implement
  }

  protected async moveStagedTranscomEventsToPublished() {
    await this.updateDbControlTableEntry(["transcom_events_expanded_publish"], {
      start_timestamp: new Date(),
    });

    const transcomEventsExpandedDbCols = await dama_meta.getTableColumns(
      "_transcom_admin",
      "transcom_events_expanded"
    );

    const indent = "\t".repeat(8);

    const conflictActionsHolders = transcomEventsExpandedDbCols
      .map(() => `${indent}%I = EXCLUDED.%I`)
      .join(",\n");

    const conflictActionFillers = transcomEventsExpandedDbCols.reduce(
      (acc: string[], col) => {
        acc.push(col);
        acc.push(col);
        return acc;
      },
      []
    );

    const sql = pgFormat(
      `
        INSERT INTO _transcom_admin.transcom_events_expanded
          SELECT
              *
            FROM _transcom_admin.%I
            ON CONFLICT ON CONSTRAINT transcom_events_expanded_pkey
              DO UPDATE
                SET -- SEE: https://stackoverflow.com/a/40689501/3970755
                  ${conflictActionsHolders}
        ; 
      `,
      this.transcomEventsExpandedStagingTableName,
      ...conflictActionFillers
    );

    logger.silly(sql);

    await dama_db.query(sql);

    await dama_db.query("CLUSTER _transcom_admin.transcom_events_expanded");

    await this.updateDbControlTableEntry(
      ["transcom_events_expanded_publish", "end_timestamp"],
      new Date()
    );
  }

  protected async clusterPublishedTranscomEvents() {
    await this.updateDbControlTableEntry(["transcom_events_expanded_cluster"], {
      start_timestamp: new Date(),
    });

    await dama_db.query("CLUSTER _transcom_admin.transcom_events_expanded");

    await this.updateDbControlTableEntry(
      ["transcom_events_expanded_cluster", "end_timestamp"],
      new Date()
    );
  }

  protected async updateTranscomEventsAdminGeographies() {
    const ctrlr = new TranscomEventsToAdminGeographiesController(
      dama_db,
      this.etlControlId
    );

    await ctrlr.run();
  }

  protected async updateTranscomEventsToConflationMap() {
    const ctrlr = new TranscomEventsToConflationMapController(
      dama_db,
      this.etlControlId
    );

    await ctrlr.run();
  }

  protected async analyzeTranscomEventsExpandedTable() {
    await dama_db.query("ANALYZE _transcom_admin.transcom_events_expanded");
  }

  protected async updateDataManagerStatistics() {
    await dama_db.query(
      "CALL _transcom_admin.update_data_manager_transcom_events_aggregate_statistics() ;"
    );
  }

  protected async beginAggregateUpdateTransaction() {
    await dama_db.query("BEGIN;");
  }

  protected async commitAggregateUpdateTransaction() {
    await dama_db.query("COMMIT;");
  }

  protected async cleanUp() {
    await this.updateDbControlTableEntry(["status"], "DONE");
    await this.updateDbControlTableEntry(["etlEnd"], this.etlEnd);
    await this.closeDbControlTableEntry();

    // this.disableLogging();

    rmSync(this.etlWorkDir, { recursive: true });
  }

  async run() {
    try {
      await this.initializeRequestedTranscomEventsDateExtent();

      await this.downloadTranscomEvents();
      await this.downloadTranscomEventsExpanded();
      await this.stageTranscomEventsExpanded();

      await this.doTranscomEventsQA();

      // BEGIN AGGREGATE BOUNDARY
      dama_db.runInTransactionContext(async () => {
        await this.moveStagedTranscomEventsToPublished();
        await this.clusterPublishedTranscomEvents();

        await Promise.all([
          this.updateTranscomEventsAdminGeographies(),
          this.updateTranscomEventsToConflationMap(),
        ]);

        await this.setDbControlTableEtlSummary();
        await this.dropTranscomEventsExpandedStagingTable();
      });
      // END AGGREGATE BOUNDARY

      // Cannot call ANALYZE within a TRANSACTION
      await this.analyzeTranscomEventsExpandedTable();
      await this.updateDataManagerStatistics();

      this.etlEnd = new Date();

      await this.cleanUp();

      logger.info("done");
    } catch (err) {
      // @ts-ignore
      logger.error(err.message);

      throw err;
    }
  }
}
