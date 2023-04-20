import { mkdirSync, appendFileSync } from "fs";
import { join } from "path";

// We're using pg-native for admin  because it offers synchronous queries.
import { default as NodePgNativeClient } from "pg-native";

import dedent from "dedent";
import pgFormat from "pg-format";

import damaHost from "../../../../src/constants/damaHost";

import {
  NodePgClient,
  getConnectedNodePgClient,
  getPostgresConnectionUri,
} from "../../../../src/data_manager/dama_db/postgres/PostgreSQL";

import { DamaTaskExitCodes } from "./types";

const {
  env: { AVAIL_DAMA_PG_ENV, AVAIL_DAMA_ETL_CONTEXT_ID },
} = process;

const ETL_CONTEXT_ID = +AVAIL_DAMA_ETL_CONTEXT_ID;

if (!AVAIL_DAMA_ETL_CONTEXT_ID || !Number.isFinite(ETL_CONTEXT_ID)) {
  throw new Error(
    `Invalid process.env.AVAIL_DAMA_ETL_CONTEXT_ID: '${AVAIL_DAMA_ETL_CONTEXT_ID}'`
  );
}

const LOGS_DIR = join(__dirname, "logs");

mkdirSync(LOGS_DIR, { recursive: true });

const LOG_FILE_PATH = join(LOGS_DIR, `etl_context.${ETL_CONTEXT_ID}.log`);

appendFileSync(
  LOG_FILE_PATH,
  `${new Date().toISOString()}: TaskManager boot\n`
);

class TaskManager {
  private _ctx_lock_cxn!: any;
  private _work_db_cxn!: Promise<NodePgClient>;
  public ready: Promise<void>;
  private readonly _initial_event: Promise<any>;

  constructor() {
    this.log(`ETL_CONTEXT_ID: ${ETL_CONTEXT_ID}`);

    // NOTE: Using pg-native so query is synchronous.
    this._initial_event = this.getLockedInitalEvent();

    this.ready = this.initialize();
  }

  shutdown(exit_code = 0) {
    try {
      // To avoid "unexpected EOF on client connection with an open transaction" PostgreSQL log messages.
      this.releaseInitialEventLock();
    } catch (err) {
      //
    }
    process.exit(exit_code);
  }

  log(msg: string) {
    appendFileSync(LOG_FILE_PATH, msg);
    appendFileSync(LOG_FILE_PATH, "\n");
  }

  async getDb() {
    return await this._work_db_cxn;
  }

  private async initialize() {
    this._work_db_cxn = getConnectedNodePgClient(AVAIL_DAMA_PG_ENV);

    console.log(
      "INITIALIZING DamaTask for ETL_CONTEXT_ID",
      AVAIL_DAMA_ETL_CONTEXT_ID
    );
    // Make sure we can lock the INITIAL event
    await this.getInitialEvent();

    await this.exitIfTaskIsDone();

    console.log(
      "INITIALIZED DamaTask for ETL_CONTEXT_ID",
      AVAIL_DAMA_ETL_CONTEXT_ID
    );
  }

  //  This method GUARANTEES that the given DamaTask is running as a SINGLETON.
  //    If the DamaTaskQueue mistakenly tries to "restart" a currently running
  //    DamaTask, the duplicate will be unable to aquire the :INTIAL event lock.
  //    The duplicate process will then exit. As the getLockedInitalEvent
  //    method is synchronous, and this file exports an instance of the
  //    TaskManager class, this provides the guarantee that as long as
  //      1. the TaskManager is imported somewhere in a DamaTask's worker code
  //      2. DamaTask work happens AFTER imports complete
  //    the DamaTask work will NOT execute, therefore safely preventing a
  //    duplicate task from corrupting the work of the older running instance.
  private getLockedInitalEvent() {
    this._ctx_lock_cxn = new NodePgNativeClient();

    this._ctx_lock_cxn.connectSync(getPostgresConnectionUri(AVAIL_DAMA_PG_ENV));

    this._ctx_lock_cxn.querySync("BEGIN ;");

    //  From https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE
    //    With SKIP LOCKED, any selected rows that cannot be immediately locked
    //    are skipped. Skipping locked rows provides an inconsistent view of the
    //    data, so this is not suitable for general purpose work, but can be used
    //    to avoid lock contention with multiple consumers accessing a queue-like
    //    table.
    const sql = dedent(`
      SELECT
          b.*
        FROM data_manager.etl_contexts AS a
          INNER JOIN data_manager.event_store AS b
            USING ( etl_context_id )
        WHERE (
          ( etl_context_id = $1 )
          AND
          ( meta->>'dama_host_id' = $2 )
          AND
          ( a.initial_event_id = b.event_id )
        )
        FOR UPDATE OF b SKIP LOCKED
    `);

    const [initial_event] = this._ctx_lock_cxn.querySync(sql, [
      ETL_CONTEXT_ID,
      damaHost,
    ]);

    if (!initial_event) {
      this.shutdown(DamaTaskExitCodes.COULD_NOT_ACQUIRE_INITIAL_EVENT_LOCK);
    }

    process.on("exit", () => {
      this.releaseInitialEventLock();
    });

    return initial_event;
  }

  // Release the :INITIAL event lock, close database connections, and prevent further work.
  private releaseInitialEventLock() {
    this.ready = Promise.reject(
      new Error(":INITIAL event lock has been released.")
    );

    this._ctx_lock_cxn?.querySync("ROLLBACK;");

    // NOTE: Not sync. https://github.com/brianc/node-pg-native#example-4
    this._ctx_lock_cxn?.end();

    this._ctx_lock_cxn = null;

    this._work_db_cxn?.then((db) => {
      db.end();
      this._work_db_cxn = null;
    });
  }

  getInitialEvent() {
    return this._initial_event;
  }

  async exitIfTaskIsDone() {
    const db = await this.getDb();

    const sql = dedent(`
      SELECT EXISTS (
        SELECT
            1
          FROM data_manager.etl_contexts
          WHERE (
            ( etl_status = 'DONE' )
            AND
            ( etl_context_id = $1 )
          )
      ) AS is_done
    `);

    this.log(sql);

    const {
      rows: [{ is_done }],
    } = await db.query({ text: sql, values: [ETL_CONTEXT_ID] });

    if (is_done) {
      this.shutdown(DamaTaskExitCodes.TASK_ALREADY_DONE);
    }
  }

  async dispatchEvent(event: any) {
    await this.ready;
    const db = await this.getDb();

    const { type, payload = null, meta = null, error = null } = event;

    console.log("ETL_CONTEXT_ID", ETL_CONTEXT_ID, "dispatching", type);

    const sql = dedent(`
      INSERT INTO data_manager.event_store (
        etl_context_id,
        type,
        payload,
        meta,
        error
      ) VALUES ( $1, $2, $3, $4, $5 )
    `);

    const values = [
      ETL_CONTEXT_ID,
      type,
      payload && JSON.stringify(payload),
      meta,
      error,
    ];

    const {
      rows: [row],
    } = await db.query({
      name: "DISPATCH EVENT",
      text: sql,
      values,
    });

    return row;
  }

  async getEtlContextEvents() {
    await this.ready;
    const db = await this.getDb();

    const sql = dedent(
      pgFormat(
        `
          SELECT
              *
            FROM data_manager.event_store
            WHERE ( etl_context_id = %s )
            ORDER BY event_id
        `,
        ETL_CONTEXT_ID
      )
    );

    this.log(sql);

    const { rows: events } = await db.query({
      name: "ALL_ETL_CONTEXT_EVENTS",
      text: sql,
    });

    return events;
  }
}

export default new TaskManager();
