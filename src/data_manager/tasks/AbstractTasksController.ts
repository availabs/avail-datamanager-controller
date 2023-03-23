import { readFile as readFileAsync } from "fs/promises";
import { join } from "path";

import execa, { ExecaError } from "execa";
import PgBoss, { WorkOptions, SendOptions } from "pg-boss";

import _ from "lodash";

import dedent from "dedent";
import pgFormat from "pg-format";

import dama_host_id from "../../constants/damaHostId";

import {
  PgEnv,
  NodePgClient,
  NodePgPoolClient,
  getPostgresConnectionUri,
} from "../dama_db/postgres/PostgreSQL";

import { DamaTaskDescriptor, DamaTaskJob, DamaTaskExitCodes } from "./domain";

const DEFAULT_QUEUE_NAME = `${dama_host_id}:DEFAULT_QUEUE`;

type PgBossWorkerInfo = { id: string; options: WorkOptions };

export default abstract class AbstractTasksController {
  private pg_boss_by_pg_env: Record<PgEnv, Promise<PgBoss> | undefined>;
  private pg_boss_workers: Record<
    PgEnv,
    Record<string, Promise<PgBossWorkerInfo> | undefined>
  >;

  constructor() {
    this.pg_boss_by_pg_env = {};
    this.pg_boss_workers = {};
  }

  // Allow getting connections through dama_db when attached to Moleculer,
  // or directly when within detached tasks.
  protected abstract getDbConnection(
    pg_env: PgEnv
  ): Promise<NodePgClient | NodePgPoolClient>;

  protected abstract releaseDbConnection(
    db: NodePgClient | NodePgPoolClient
  ): Promise<void>;

  private async getPgBoss(pg_env: PgEnv): Promise<PgBoss> {
    // IDEMPOTENCY: If there is Promise assigned for this PgEnv, we've already started initialization.
    if (this.pg_boss_by_pg_env[pg_env]) {
      return <PgBoss>await this.pg_boss_by_pg_env[pg_env];
    }

    let done: (pg_boss: PgBoss) => PgBoss;
    let fail: Function;

    this.pg_boss_by_pg_env[pg_env] = new Promise((resolve, reject) => {
      // @ts-ignore
      done = resolve;
      fail = reject;
    });

    const db = await this.getDbConnection(pg_env);

    try {
      await db.query("CREATE EXTENSION IF NOT EXISTS pgcrypto;");

      const pgConnectionUri = getPostgresConnectionUri(pg_env);

      // NOTE: pg-boss maintains a long-running connection
      const pg_boss = new PgBoss(pgConnectionUri);

      //  Note: This will create the pgboss SCHEMA an its TABLES
      //        See https://github.com/timgit/pg-boss/blob/master/docs/readme.md#start
      await pg_boss.start();

      //  CREATE the data_manager.dama_task_queue VIEW
      //    that JOINs the pgboss TABLEs with the etl_contexts TABLE.
      const fpath = join(__dirname, "./sql/create_dama_pgboss_view.sql");
      const sql = await readFileAsync(fpath, { encoding: "utf8" });

      await db.query(sql);

      // process.nextTick() so the original caller of this method proceeds first.
      process.nextTick(() => done(pg_boss));

      return pg_boss;
    } catch (err) {
      process.nextTick(() => fail(err));
      throw err;
    } finally {
      await this.releaseDbConnection(db);
    }
  }

  prefixDamaTaskQueueNameWithHostId(dama_task_queue_name: string) {
    const re = new RegExp(`^${dama_host_id}`);

    if (!re.test(dama_task_queue_name)) {
      return `${dama_host_id}:${dama_task_queue_name}`;
    }

    return dama_task_queue_name;
  }

  async registerTaskQueue(
    pg_env: PgEnv,
    dama_task_queue_name: string,
    options: WorkOptions = {}
  ) {
    dama_task_queue_name =
      this.prefixDamaTaskQueueNameWithHostId(dama_task_queue_name);

    if (this.pg_boss_workers[pg_env]?.[dama_task_queue_name]) {
      const { id, options: orig_opts } = <PgBossWorkerInfo>(
        await this.pg_boss_workers[pg_env]?.[dama_task_queue_name]
      );

      if (!_.isEqual(options, orig_opts)) {
        throw new Error(
          "TaskQueue has already been registered with different WorkOptions."
        );
      }

      return id;
    }

    this.pg_boss_workers[pg_env] = this.pg_boss_workers[pg_env] || {};

    let done: (info: PgBossWorkerInfo) => PgBossWorkerInfo;
    let fail: Function;

    this.pg_boss_workers[pg_env][dama_task_queue_name] = new Promise(
      (resolve, reject) => {
        // @ts-ignore
        done = resolve;
        fail = reject;
      }
    );

    try {
      const boss = await this.getPgBoss(pg_env);

      const id = await boss.work(
        dama_task_queue_name,
        options,
        this.startDamaTask.bind(this, pg_env)
      );

      process.nextTick(() => done({ id, options }));

      return id;
    } catch (err) {
      process.nextTick(() => fail(err));
      throw err;
    }
  }

  taskQueueIsRegistered(
    pg_env: PgEnv,
    dama_task_queue_name = DEFAULT_QUEUE_NAME
  ) {
    dama_task_queue_name =
      this.prefixDamaTaskQueueNameWithHostId(dama_task_queue_name);

    const id_promise = this.pg_boss_workers[pg_env]?.[dama_task_queue_name];

    return !!id_promise;
  }

  private async startDamaTask(pg_env: PgEnv, dama_job: DamaTaskJob) {
    const {
      id: AVAIL_DAMA_TASK_ID,
      data: { etl_context_id, worker_path },
    } = dama_job;

    if (!etl_context_id) {
      throw new Error("ALL DamaTasks MUST have an etl_context_id");
    }

    console.log("DamaTask for EtlContext", etl_context_id, "STARTED");

    const AVAIL_DAMA_ETL_CONTEXT_ID = `${etl_context_id}`;

    const run_in_debug_mode =
      process.env.AVAIL_DAMA_TASKS_DEBUG_MODE === "1" ||
      process.env.AVAIL_DAMA_TASKS_DEBUG_MODE === "true";

    const detached = run_in_debug_mode;
    const stdio = run_in_debug_mode ? "inherit" : "ignore";

    try {
      const task_process = execa(worker_path, {
        env: {
          ...process.env,
          AVAIL_DAMA_PG_ENV: pg_env,
          AVAIL_DAMA_TASK_ID,
          AVAIL_DAMA_ETL_CONTEXT_ID,
          AVAIL_DAMA_TASK_CONTROLLER_PID: `${process.pid}`,
        },

        detached,
        stdio,
      });

      await task_process;

      return;
    } catch (err) {
      console.error(err);
      console.error(`ExecaErrorCode: ${(<ExecaError>err).exitCode}`);

      // @ts-ignore
      switch (err.exitCode) {
        case DamaTaskExitCodes.TASK_ALREADY_DONE:
          console.log("==> TASK_ALREADY_DONE");
          return null;
        case DamaTaskExitCodes.COULD_NOT_ACQUIRE_INITIAL_EVENT_LOCK:
          console.log("==> COULD_NOT_ACQUIRE_INITIAL_EVENT_LOCK");
          return await this.handleDuplicateTask(pg_env, etl_context_id);
        default:
          throw err;
      }
    }
  }

  private async damaTaskInitialEventLockIsLocked(
    pg_env: PgEnv,
    etl_context_id: number
  ) {
    const db = await this.getDbConnection(pg_env);

    const sql = dedent(
      pgFormat(
        `
          BEGIN ;

          SELECT EXISTS (
            SELECT
                1
              FROM data_manager.etl_contexts
              WHERE (
                ( etl_context_id = %s )
                AND
                ( initial_event_id IS NOT NULL )
              )
          ) AS etl_context_exists ;

          SELECT NOT EXISTS (
            SELECT
                a.etl_context_id,
                b.event_id
              FROM data_manager.etl_contexts AS a
                INNER JOIN data_manager.event_store AS b
                  USING ( etl_context_id )
              WHERE (
                ( a.etl_context_id = %s )
                AND
                ( a.initial_event_id = b.event_id )
              )
              FOR UPDATE OF b SKIP LOCKED
          ) AS unable_to_select_initial_event;

          ROLLBACK ;
        `,
        etl_context_id,
        etl_context_id
      )
    );

    // @ts-ignore
    const [
      ,
      {
        rows: [{ etl_context_exists }],
      },
      {
        rows: [{ unable_to_select_initial_event }],
      },
    ] = await db.query(sql);

    await this.releaseDbConnection(db);

    return etl_context_exists && unable_to_select_initial_event;
  }

  // FIXME: This method may unnecessarily hold onto a connection for a long time.
  //        It would be better to have a simple query method.
  private async handleDuplicateTask(pg_env: PgEnv, etl_context_id: number) {
    // poll the :INITIAL event lock until the running DamaTask releases it.
    while (
      await this.damaTaskInitialEventLockIsLocked(pg_env, etl_context_id)
    ) {
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }

    const db = await this.getDbConnection(pg_env);

    const sql = dedent(`
      SELECT
          etl_status
        FROM data_manager.etl_contexts
        WHERE ( etl_context_id = $1 )
    `);

    const {
      rows: [{ etl_status }],
    } = await db.query(sql, [etl_context_id]);

    await this.releaseDbConnection(db);

    console.log("==> EtlContext", etl_context_id, "etl_status =", etl_status);

    if (etl_status !== "DONE") {
      throw new Error("DamaTask failed.");
    }

    return null;
  }

  async queueDamaTask(
    pg_env: PgEnv,
    dama_task_descr: DamaTaskDescriptor,
    pgboss_send_options: SendOptions
  ) {
    const {
      dama_task_queue_name = DEFAULT_QUEUE_NAME,
      parent_context_id = null,
      source_id = null,
      initial_event,
      worker_path,
    } = dama_task_descr;

    const _dama_task_queue_name =
      this.prefixDamaTaskQueueNameWithHostId(dama_task_queue_name);

    console.log("\n\n==> dama_task_queue_name:", _dama_task_queue_name, "\n\n");

    if (!this.taskQueueIsRegistered(pg_env, _dama_task_queue_name)) {
      if (_dama_task_queue_name === DEFAULT_QUEUE_NAME) {
        await this.registerTaskQueue(pg_env, _dama_task_queue_name);
      } else {
        throw new Error(
          "Non-default DataManager TaskQueues MUST be registered before tasks can be queued."
        );
      }
    }

    if (!/:INITIAL$/.test(initial_event.type)) {
      throw new Error(
        "DamaTask Invariant Violation: initial_event MUST be an :INITIAL event."
      );
    }

    const db = await this.getDbConnection(pg_env);

    try {
      await db.query("BEGIN ;");

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
        dama_host_id,
        dama_task_queue_name: _dama_task_queue_name,
        pgboss_send_options,
      };

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

      await db.query("COMMIT ;");

      const pg_boss = await this.getPgBoss(pg_env);

      console.log(JSON.stringify({ pgboss_send_options }, null, 4));

      const etl_task_id = await pg_boss.send(
        _dama_task_queue_name,
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
      }

      return { etl_context_id, etl_task_id };
    } catch (err) {
      console.error(err);
      await db.query("ROLLACK ;");
    } finally {
      this.releaseDbConnection(db);
    }
  }

  async getDamaTaskStatus(pg_env: PgEnv, etl_context_id: number) {
    const db = await this.getDbConnection(pg_env);

    try {
      const sql = dedent(`
        SELECT
            *
          FROM data_manager.dama_task_queue
          WHERE ( etl_context_id = $1 )
      `);

      const query = { text: sql, values: [etl_context_id] };

      console.log(JSON.stringify({ query }, null, 4));

      const { rows } = await db.query(query);

      if (!rows.length) {
        throw new Error(`No such etl_context_id: ${etl_context_id}`);
      }

      return rows[0];
    } finally {
      this.releaseDbConnection(db);
    }
  }
}
