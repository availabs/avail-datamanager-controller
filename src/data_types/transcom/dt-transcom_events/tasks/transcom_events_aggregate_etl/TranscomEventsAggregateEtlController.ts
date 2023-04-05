/*
    TODO: 2nd DB connection to update control table so updates persist if main transaction fails
*/

import * as os from "os";
import { mkdirSync, rmSync, existsSync, createWriteStream } from "fs";
import { join } from "path";

import pgFormat from "pg-format";

import { getConnectedPgClient } from "../../utils/PostgreSQL";

import initializeTranscomDatabaseTables from "../db_admin/tasks/initializeTranscomDatabaseTables";
import {
  downloadTranscomEvents,
  makeTranscomEventIdIteratorFromApiScrapeDirectory,
} from "../transcom_events";

import { dbCols as transcomEventsExpandedDbCols } from "../transcom_events_expanded/data_schema";

import TranscomEventsToAdminGeographiesController from "./TranscomEventsToAdminGeographiesController";
import TranscomEventsToConflationMapController from "./TranscomEventsToConflationMapController";

import {
  downloadTranscomEventsExpanded,
  loadApiScrapeDirectoryIntoDatabase,
} from "../transcom_events_expanded";

import {
  getTimestamp,
  getTranscomRequestFormattedTimestamp,
} from "../utils/dates";
import { echoConsoleToWriteStream } from "../utils/logging";

import { PgEnv, PgClient } from "../../domain/PostgreSQLTypes";

const etlBaseDir = join(__dirname, "../../../", "etl-work-dirs");

export default class TranscomEventsAggregateEtlControler {
  protected _etlStart: Date;
  protected _etlWorkDir: string;
  protected etlEnd: Date;
  protected etlTranscomEventsDir!: string;
  protected etlTranscomEventsExpandedDir!: string;
  protected etlHostname: string;
  protected etlControlId: number;
  protected disableLogging: Function | null;
  protected _db?: PgClient | null;
  protected transcomEventsExpandedStagingTableName?: string;

  constructor(
    protected readonly pgEnv: PgEnv,
    protected eventsStartTime: Date | null = null,
    protected eventsEndTime: Date | null = null
  ) {
    this.etlHostname = os.hostname() || "unknown-host-name";
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

  protected async getDbConnection() {
    if (this._db) {
      return this._db;
    }

    if (this._db === null) {
      throw new Error("The DB Connection as been closed.");
    }

    this._db = await getConnectedPgClient(this.pgEnv);

    return this._db;
  }

  protected async closeDbConnection() {
    if (this._db !== null) {
      const db = this._db;
      this._db = null;

      console.log("CLOSE CONNECTION");
      await db.end();
    }
  }

  protected initializeEtlWorkDir() {}

  protected initializeLogging() {
    if (this.disableLogging) {
      return;
    }

    const logFilePath = join(this.etlWorkDir, "aggregate_etl.log");

    const logStream = createWriteStream(logFilePath);

    const disableEchoing = echoConsoleToWriteStream(logStream);

    console.log("Echoing process STDOUT and STDERR to", logFilePath);

    this.disableLogging = () => {
      disableEchoing();
      logStream.close();
      this.disableLogging = null;
    };
  }

  protected initializeTranscomDatabaseTables() {
    initializeTranscomDatabaseTables(this.pgEnv);
  }

  async getTranscomEventsLatestStartDateTimeFromDatabase() {
    const db = await this.getDbConnection();
    const {
      rows: [{ latest_start_time }],
    } = await db.query(`
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
      etlHostname: this.etlHostname,
      etlWorkDir: this.etlWorkDir,
      status: "IN_PROGRESS",
      subprocesses: [],
    };
  }

  protected async initializeDbControlTableEntry() {
    const db = await this.getDbConnection();

    const {
      rows: [{ id }],
    } = await db.query(
      `
        INSERT INTO _transcom_admin.etl_control (
          start_timestamp,
          metadata
        ) VALUES ($1, $2)
          RETURNING id
      `,
      [this.etlStart, JSON.stringify(this.initialEtlControlMetadata)]
    );

    this.etlControlId = id;
  }

  protected async updateDbControlTableEntry(path: string[], value: any) {
    const db = await this.getDbConnection();

    await db.query(
      `
        UPDATE _transcom_admin.etl_control
          SET metadata = jsonb_set(metadata, $1, $2, true)
          WHERE ( id = $3 )
      `,
      [path, JSON.stringify(value), this.etlControlId]
    );
  }

  protected async closeDbControlTableEntry() {
    const db = await this.getDbConnection();

    await db.query(
      `
        UPDATE _transcom_admin.etl_control
          SET end_timestamp = $1
          WHERE ( id = $2 )
      `,
      [this.etlEnd, this.etlControlId]
    );
  }

  protected async downloadTranscomEvents() {
    await this.updateDbControlTableEntry(["transcom_events_download"], {
      start_timestamp: new Date(),
    });

    await downloadTranscomEvents(
      getTranscomRequestFormattedTimestamp(this.eventsStartTime),
      getTranscomRequestFormattedTimestamp(this.eventsEndTime),
      this.etlTranscomEventsDir
    );

    await this.updateDbControlTableEntry(
      ["transcom_events_download", "end_timestamp"],
      new Date()
    );
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

    await this.updateDbControlTableEntry(
      ["transcom_events_expanded_download"],
      {
        start_timestamp: new Date(),
      }
    );

    await downloadTranscomEventsExpanded(
      this.transcomEventIdAsyncIteratorFromApiScrapeDirectory,
      this.etlTranscomEventsExpandedDir
    );

    await this.updateDbControlTableEntry(
      ["transcom_events_expanded_download", "end_timestamp"],
      new Date()
    );
  }

  protected async createTranscomEventsExpandedStagingTable() {
    const db = await this.getDbConnection();

    this.transcomEventsExpandedStagingTableName = `staged_transcom_events_etl_id_${this.etlControlId}`;

    const sql = pgFormat(
      `
        CREATE TABLE _transcom_admin.%I (
          LIKE _transcom_admin.transcom_events_expanded
            INCLUDING DEFAULTS      -- necessary for _created_timestamp & _modified_timestamp
            EXCLUDING CONSTRAINTS   -- because scrapes may violate PrimaryKey CONSTRAINT
        ) ;
      `,
      this.transcomEventsExpandedStagingTableName
    );

    await db.query(sql);
  }

  protected async dropTranscomEventsExpandedStagingTable() {
    const db = await this.getDbConnection();

    this.transcomEventsExpandedStagingTableName = `staged_transcom_events_etl_id_${this.etlControlId}`;

    const sql = pgFormat(
      "DROP TABLE _transcom_admin.%I ;",
      this.transcomEventsExpandedStagingTableName
    );

    await db.query(sql);
  }

  protected async stageTranscomEventsExpanded() {
    const start_timestamp = new Date();
    await this.updateDbControlTableEntry(["transcom_events_expanded_staging"], {
      start_timestamp,
    });

    await this.createTranscomEventsExpandedStagingTable();

    const db = await this.getDbConnection();

    await loadApiScrapeDirectoryIntoDatabase(
      this.etlTranscomEventsExpandedDir,
      "_transcom_admin",
      this.transcomEventsExpandedStagingTableName,
      db
    );

    await this.updateDbControlTableEntry(["transcom_events_expanded_staging"], {
      start_timestamp,
      end_timestamp: new Date(),
    });
  }

  protected async setDbControlTableEtlSummary() {
    const db = await this.getDbConnection();

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
    } = await db.query(sql);

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

    const db = await this.getDbConnection();

    const indent = `\t`.repeat(8);

    const conflictActionsHolders = transcomEventsExpandedDbCols
      .map(() => `${indent}%I = EXCLUDED.%I`)
      .join(`,\n`);

    const conflictActionFillers = transcomEventsExpandedDbCols.reduce(
      (acc, col) => {
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

    await db.query(sql);

    await db.query("CLUSTER _transcom_admin.transcom_events_expanded");

    await this.updateDbControlTableEntry(
      ["transcom_events_expanded_publish", "end_timestamp"],
      new Date()
    );
  }

  protected async clusterPublishedTranscomEvents() {
    await this.updateDbControlTableEntry(["transcom_events_expanded_cluster"], {
      start_timestamp: new Date(),
    });

    const db = await this.getDbConnection();

    await db.query("CLUSTER _transcom_admin.transcom_events_expanded");

    await this.updateDbControlTableEntry(
      ["transcom_events_expanded_cluster", "end_timestamp"],
      new Date()
    );
  }

  protected async updateTranscomEventsAdminGeographies() {
    const db = await this.getDbConnection();
    const ctrlr = new TranscomEventsToAdminGeographiesController(
      db,
      this.etlControlId
    );

    await ctrlr.run();
  }

  protected async updateTranscomEventsToConflationMap() {
    const db = await this.getDbConnection();
    const ctrlr = new TranscomEventsToConflationMapController(
      db,
      this.etlControlId
    );

    await ctrlr.run();
  }

  protected async analyzeTranscomEventsExpandedTable() {
    const db = await this.getDbConnection();

    await db.query("ANALYZE _transcom_admin.transcom_events_expanded");
  }

  protected async updateDataManagerStatistics() {
    const db = await this.getDbConnection();

    await db.query(
      "CALL _transcom_admin.update_data_manager_transcom_events_aggregate_statistics() ;"
    );
  }

  protected async beginAggregateUpdateTransaction() {
    const db = await this.getDbConnection();

    await db.query("BEGIN;");
  }

  protected async commitAggregateUpdateTransaction() {
    const db = await this.getDbConnection();

    await db.query("COMMIT;");
  }

  protected async cleanUp() {
    await this.updateDbControlTableEntry(["status"], "DONE");
    await this.updateDbControlTableEntry(["etlEnd"], this.etlEnd);
    await this.closeDbControlTableEntry();

    await this.closeDbConnection();
    // this.disableLogging();

    rmSync(this.etlWorkDir, { recursive: true });
  }

  async run() {
    // this.initializeLogging();
    try {
      this.initializeTranscomDatabaseTables();

      await this.initializeRequestedTranscomEventsDateExtent();
      await this.initializeDbControlTableEntry();

      await this.downloadTranscomEvents();
      await this.downloadTranscomEventsExpanded();
      await this.stageTranscomEventsExpanded();

      await this.doTranscomEventsQA();

      // BEGIN AGGREGATE BOUNDARY
      await this.beginAggregateUpdateTransaction();

      await this.moveStagedTranscomEventsToPublished();
      await this.clusterPublishedTranscomEvents();

      await Promise.all([
        this.updateTranscomEventsAdminGeographies(),
        this.updateTranscomEventsToConflationMap(),
      ]);

      await this.commitAggregateUpdateTransaction();

      await this.setDbControlTableEtlSummary();
      await this.dropTranscomEventsExpandedStagingTable();
      // END AGGREGATE BOUNDARY

      // Cannot call ANALYZE within a TRANSACTION
      await this.analyzeTranscomEventsExpandedTable();
      await this.updateDataManagerStatistics();

      this.etlEnd = new Date();

      await this.cleanUp();

      console.log("done");
    } catch (err) {
      console.error(err);
      await this.closeDbConnection();
      throw err;
    }
  }
}
