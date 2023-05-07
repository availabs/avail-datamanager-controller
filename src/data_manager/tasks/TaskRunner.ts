/* eslint-disable no-unused-expressions */

/*
  Running a DamaTask for debugging.

    1. Do not start any queue workers.
    2. Queue the task.
    3. Start the TaskRunner yourself:

        $ AVAIL_DAMA_PG_ENV=dama_dev_1 \
          AVAIL_DAMA_ETL_CONTEXT_ID=386 \
          node \
            --require ts-node/register \
            src/data_manager/tasks/TaskRunner.ts
*/

import dedent from "dedent";

import dama_host_id from "../../constants/damaHostId";

import {
  NodePgClient,
  getConnectedNodePgClient,
} from "../dama_db/postgres/PostgreSQL";

import dama_db from "../dama_db";
import dama_events, { DamaEvent } from "../events";
import { getLoggerForContext, Logger } from "../logger";

import { TaskEtlContext } from "../contexts";

import { DamaTaskExitCodes } from "./domain";

const {
  env: { AVAIL_DAMA_PG_ENV, AVAIL_DAMA_ETL_CONTEXT_ID },
} = process;

if (!AVAIL_DAMA_PG_ENV) {
  throw new Error("No AVAIL_DAMA_PG_ENV ENV variable.");
}

const PG_ENV = <string>AVAIL_DAMA_PG_ENV;

// @ts-ignore
const ETL_CONTEXT_ID = +AVAIL_DAMA_ETL_CONTEXT_ID;

if (!AVAIL_DAMA_ETL_CONTEXT_ID || !Number.isFinite(ETL_CONTEXT_ID)) {
  throw new Error(
    `Invalid process.env.AVAIL_DAMA_ETL_CONTEXT_ID: '${AVAIL_DAMA_ETL_CONTEXT_ID}'`
  );
}

class TaskRunner {
  private _ctx_lock_cxn!: NodePgClient;
  private readonly initial_event!: DamaEvent;
  private logger: Logger;

  constructor() {
    // CONSIDER:  Logging before :INITIAL is locked would cause a duplicate task
    //            to modify the original task's log, which would be confusing
    this.logger = getLoggerForContext(ETL_CONTEXT_ID, PG_ENV);
  }

  async shutdown(exit_code = 0) {
    try {
      // To avoid "unexpected EOF on client connection with an open transaction" PostgreSQL log messages.
      await this.releaseInitialEventLock();
    } catch (err) {
      //
    }

    process.exit(exit_code);
  }

  async run() {
    this.logger.info(`==> TaskRunner.run() pid=${process.pid}`);

    try {
      // @ts-ignore
      this.initial_event = await this.getLockedInitalEvent();
    } catch (err) {
      console.error(err);
    }

    await this.exitIfTaskIsDone();

    const {
      meta: {
        // @ts-ignore
        // @ts-ignore
        __dama_task_manager__: { worker_path, etl_work_dir },
      },
    } = this.initial_event;

    this.logger.debug(`==> TaskRunner worker_path=${worker_path}`);

    try {
      // https://mariusschulz.com/blog/dynamic-import-expressions-in-typescript
      const {
        default: main,
      }: {
        default: (
          etl_ctx: TaskEtlContext
        ) => DamaEvent | Promise<DamaEvent> | unknown;
      } = await import(worker_path);

      const etl_ctx = {
        initial_event: this.initial_event,
        logger: this.logger,
        meta: { pgEnv: PG_ENV, etl_context_id: ETL_CONTEXT_ID, etl_work_dir },
      };

      const final_event = await main(etl_ctx);

      // If the worker dispatched it's :FINAL event, we are done.
      await this.exitIfTaskIsDone();

      // @ts-ignore
      if (!/:FINAL$/.test(final_event?.type)) {
        this.shutdown(DamaTaskExitCodes.WORKER_DID_NOT_RETURN_FINAL_EVENT);
      }

      await dama_events.dispatch(
        <DamaEvent>final_event,
        ETL_CONTEXT_ID,
        PG_ENV
      );

      await this.shutdown(DamaTaskExitCodes.DONE);
    } catch (err) {
      this.logger.error((<Error>err).message);
      this.logger.error((<Error>err).stack);

      const payload = {
        message: (<Error>err).message,
        stack: (<Error>err).stack,
      };

      dama_events.dispatch(
        { type: ":ERROR", payload, error: true },
        ETL_CONTEXT_ID,
        PG_ENV
      );

      await this.shutdown(DamaTaskExitCodes.WORKER_THREW_ERROR);
    }
  }

  //  This method GUARANTEES that the given DamaTask is running as a SINGLETON
  //    for only a single process can aquire the :INITIAL event lock.
  //
  //    If the DamaTaskQueue mistakenly tries to "restart" a currently running
  //    DamaTask, the duplicate will be unable to aquire the :INTIAL event lock.
  //    The duplicate process then MUST exit immediately.
  //
  //    As the getLockedInitalEvent method is synchronous, and this file
  //    exports an instance of the TaskManager class, this provides the
  //    guarantee that so long as
  //
  //      1. the TaskManager is imported somewhere in a DamaTask's worker code
  //      2. ANY DamaTask work happens AFTER the import of this module is complete
  //
  //    duplicate DamaTasks work will NOT have ANY side effects,
  //    therefore safely preventing a duplicate task from corrupting the work
  //    of the SINGLETON instance.
  private async getLockedInitalEvent() {
    this.logger.debug("Aquiring :INITIAL lock");
    this._ctx_lock_cxn = await getConnectedNodePgClient(PG_ENV);

    await this._ctx_lock_cxn.query("BEGIN ;");

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
          -- NOTE: Can obtain :INITIAL event lock IFF Task was queued/scheduled on same dama_host.
          --       This prevents dev laptops from picking up production ETL tasks.
          ( meta->'__dama_task_manager__'->>'dama_host_id' = $2 )
          AND
          ( a.initial_event_id = b.event_id )
        )
        FOR UPDATE OF b SKIP LOCKED
    `);

    this.logger.silly("before SELECT FOR UPDATE");
    const {
      rows: [initial_event],
    } = await this._ctx_lock_cxn.query(sql, [ETL_CONTEXT_ID, dama_host_id]);
    this.logger.silly("after SELECT FOR UPDATE");

    if (!initial_event) {
      this.logger.error("Unable to aquire :INITIAL event lock.");

      await this.shutdown(
        DamaTaskExitCodes.COULD_NOT_ACQUIRE_INITIAL_EVENT_LOCK
      );
    }

    this.logger.debug("Aquired :INITIAL lock");

    // NOTE: exit handlers don't support async. The results of this will be non-determinant.
    process.on("exit", this.shutdown.bind(this));

    return initial_event;
  }

  // Release the :INITIAL event lock, close database connections, and prevent further work.
  private async releaseInitialEventLock() {
    if (this._ctx_lock_cxn) {
      this.logger.debug("Releasing :INITIAL lock");

      await this._ctx_lock_cxn?.query("ROLLBACK;");

      // NOTE: Not sync. https://github.com/brianc/node-pg-native#example-4
      await this._ctx_lock_cxn?.end();

      // @ts-ignore
      this._ctx_lock_cxn = null;
    }
  }

  async taskIsDone() {
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

    const {
      rows: [{ is_done }],
    } = await dama_db.query({ text: sql, values: [ETL_CONTEXT_ID] }, PG_ENV);

    if (is_done) {
      this.logger.debug("Task is done");
    }

    return is_done;
  }

  async exitIfTaskIsDone() {
    const is_done = await this.taskIsDone();

    if (is_done) {
      await this.shutdown(DamaTaskExitCodes.DONE);
    }
  }
}

new TaskRunner().run();
