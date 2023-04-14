import { readFile as readFileAsync } from "fs/promises";
import { join, isAbsolute } from "path";

import PgBoss, { SendOptions as PgBossSendOptions } from "pg-boss";

import { v4 as uuid } from "uuid";
import _ from "lodash";
import { table } from "table";
import { Lock } from "semaphore-async-await";

import dedent from "dedent";

import dama_db from "../dama_db";

import dama_host_id from "../../constants/damaHostId";

import {
  PgEnv,
  getPostgresConnectionUri,
} from "../dama_db/postgres/PostgreSQL";

import DamaContextAttachedResource from "../contexts/index";

import { DamaTaskQueueName, DamaTaskDescriptor } from "./domain";

const DEFAULT_QUEUE_NAME = `${dama_host_id}:DEFAULT_QUEUE`;

// This class should be used in DamaTasks to queue DamaSubTasks.
// Executing tasks will be handled by the DamaController process.
export default class BaseTasksController extends DamaContextAttachedResource {
  protected readonly pgboss_by_pgenv: Record<
    PgEnv,
    Promise<PgBoss> | undefined
  >;

  private readonly queue_lock: Lock;

  constructor() {
    super();
    this.pgboss_by_pgenv = {};
    this.queue_lock = new Lock();
  }

  //  By prefixing task queue names with the host_id,
  //    we ensure the tasks run ONLY on the machines where they were queued.
  //  This will prevent local dev laptops from picking up production jobs,
  //    and vice versa, if they connect to the same databases.
  prefixDamaTaskQueueNameWithHostId(dama_task_queue_name: DamaTaskQueueName) {
    const re = new RegExp(`^${dama_host_id}:`);

    if (!re.test(dama_task_queue_name)) {
      return `${dama_host_id}:${dama_task_queue_name}`;
    }

    return dama_task_queue_name;
  }

  protected async getPgBoss(pg_env = this.pg_env): Promise<PgBoss> {
    // IDEMPOTENCY: If there is Promise assigned for this PgEnv, we've already started initialization.
    if (this.pgboss_by_pgenv[pg_env]) {
      return <PgBoss>await this.pgboss_by_pgenv[pg_env];
    }

    let done: (pgboss: PgBoss) => PgBoss;
    let fail: Function;

    this.pgboss_by_pgenv[pg_env] = new Promise((resolve, reject) => {
      // @ts-ignore
      done = resolve;
      fail = reject;
    });

    const db = await dama_db.getDbConnection(pg_env);

    try {
      await db.query("CREATE EXTENSION IF NOT EXISTS pgcrypto;");

      const pgConnectionUri = getPostgresConnectionUri(pg_env);

      // NOTE: pg-boss maintains a long-running connection
      const pgboss = new PgBoss(pgConnectionUri);

      //  Note: This will create the pgboss SCHEMA and its TABLES
      //        See https://github.com/timgit/pg-boss/blob/master/docs/readme.md#start
      await pgboss.start();

      //  CREATE the data_manager.dama_task_queue VIEW
      //    that JOINs the pgboss TABLEs with the etl_contexts TABLE.
      const fpath = join(__dirname, "./sql/create_dama_pgboss_view.sql");
      const sql = await readFileAsync(fpath, { encoding: "utf8" });

      await db.query(sql);

      // process.nextTick() so the original caller of this method proceeds first.
      process.nextTick(() => done(pgboss));

      return pgboss;
    } catch (err) {
      process.nextTick(() => fail(err));
      throw err;
    } finally {
      db.release();
    }
  }

  async queueDamaTask(
    dama_task_descr: DamaTaskDescriptor,
    pgboss_send_options: PgBossSendOptions = {},
    pg_env = this.pg_env
  ) {
    //  pg-boss drops jobs. ??? Due to throttling ???
    //    https://github.com/timgit/pg-boss/blob/master/docs/readme.md#send
    //  This seems to fix the problem, here.
    //    If put immediately around pgboss.send, it does not.
    await this.queue_lock.acquire();

    const trace_id = uuid();

    this.logger.debug(
      `dama_queue.queueDamaTask trace_id=${trace_id} ${JSON.stringify({
        dama_task_descr,
      })}`
    );

    const {
      dama_task_queue_name = DEFAULT_QUEUE_NAME,
      parent_context_id = null,
      source_id = null,
      initial_event,
      worker_path,
    } = dama_task_descr;

    if (!worker_path) {
      throw new Error("ALL DamaTasks MUST have a worker_path.");
    }

    if (!isAbsolute(worker_path)) {
      throw new Error("ALL DamaTasks worker_path must be absolute.");
    }

    const prefixed_dama_task_queue_name =
      this.prefixDamaTaskQueueNameWithHostId(dama_task_queue_name);

    if (!/:INITIAL$/.test(initial_event.type)) {
      throw new Error(
        "DamaTask Invariant Violation: initial_event MUST be an :INITIAL event."
      );
    }

    const db = await dama_db.getDbConnection(pg_env);

    try {
      await db.query("BEGIN ;");

      // NOTE: duplicating dama_event.spawnEtlContext so we can run in TRANSACTION.
      const {
        rows: [{ etl_context_id }],
      } = await db.query({
        text: dedent(`
          INSERT INTO data_manager.etl_contexts (
            parent_context_id,
            source_id
          ) VALUES ( $1, $2 )
          RETURNING etl_context_id
        `),
        values: [parent_context_id, source_id],
      });

      const { type = null, payload = null, meta = {} } = initial_event;

      const task_meta = {
        ...meta,
        __dama_task_manager__: {
          worker_path,
          dama_host_id,
          dama_task_queue_name: prefixed_dama_task_queue_name,
          pgboss_send_options,
        },
      };

      // NOTE: duplicating dama_event.dispatch so we can run in TRANSACTION.
      await db.query({
        text: dedent(`
          INSERT INTO data_manager.event_store (
            etl_context_id,
            type,
            payload,
            meta
          ) VALUES ( $1, $2, $3, $4 )
          RETURNING event_id
        `),
        values: [etl_context_id, type, payload, task_meta],
      });

      //  FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME
      //    It is possible to dispatch an :INITIAL event but pg-boss fails to queue task.
      //      The problem is the :INITIAL event MUST be available when the Job started,
      //      therefore we cannot wait until after pgboss.send success to dispatch :INITIAL.
      //
      //  ??? Possible fixes
      //        * Retry send, and if it fails after N retries, delete the new EtlContext.
      //          * https://github.com/timgit/pg-boss/blob/master/src/plans.js
      //        * Using pg-boss newoptions.db so inserting :INITIAL and pg-boss share a transaction?
      //          * See https://github.com/timgit/pg-boss/blob/master/docs/readme.md#newoptions
      //          * Draw back is we don't know if pg-boss will issue a BEGIN statement.
      //            * Currently, I don't see any BEGIN statement in the pg-boss SQL for queueing tasks.
      await db.query("COMMIT ;");

      const pgboss = await this.getPgBoss(pg_env);

      const etl_task_id = await pgboss.send(
        prefixed_dama_task_queue_name,
        {
          etl_context_id,
          worker_path,
        },
        pgboss_send_options
      );

      if (etl_task_id) {
        await db.query({
          text: dedent(`
            UPDATE data_manager.etl_contexts
              SET etl_task_id = $1
            WHERE ( etl_context_id = $2 )
          `),
          values: [etl_task_id, etl_context_id],
        });
      } else {
        this.logger.warn(
          `pg-boss failed to queue the task ${JSON.stringify(
            dama_task_descr,
            null,
            4
          )}`
        );
      }

      const d = table(
        [
          ["etl_context_id", "pg_env"],
          [etl_context_id, pg_env],
        ],
        { header: { alignment: "center", content: "Queued Task" } }
      );

      this.logger.info(`\n${d}\n`);
      this.logger.debug(
        `dama_tasks queueing ${JSON.stringify(
          { dama_task_descr, etl_context_id, etl_task_id },
          null,
          4
        )}`
      );

      await new Promise((resolve) => setTimeout(resolve, 3000));

      this.queue_lock.release();

      return { etl_context_id, etl_task_id };
    } catch (err) {
      console.error(err);
      await db.query("ROLLBACK ;");
    } finally {
      db.release();
    }
  }

  // For now, Tasks can poll for SubTask status.
  // Eventually we could push :FINAL events using https://www.npmjs.com/package/pg-listen
  async getDamaTaskStatus(etl_context_id: number, pg_env = this.pg_env) {
    const sql = dedent(`
      SELECT
          *
        FROM data_manager.dama_task_queue
        WHERE ( etl_context_id = $1 )
    `);

    const query = { text: sql, values: [etl_context_id] };

    const { rows } = await dama_db.query(query, pg_env);

    if (!rows.length) {
      throw new Error(`No such etl_context_id: ${etl_context_id}`);
    }

    return rows[0];
  }
}
