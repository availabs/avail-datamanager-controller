import { join } from "path";

import execa, { ExecaError } from "execa";
import {
  WorkOptions as PgBossWorkOptions,
  SendOptions as PgBossSendOptions,
  Worker as PgBossWorker,
} from "pg-boss";

import _ from "lodash";

import dedent from "dedent";
import pgFormat from "pg-format";

import dama_db from "../dama_db";

import dama_host_id from "../../constants/damaHostId";

import { NodePgPoolClient, PgEnv } from "../dama_db/postgres/PostgreSQL";

import {
  DamaTaskQueueName,
  DamaTaskDescriptor,
  DamaTaskJob,
  DamaTaskExitCodes,
} from "./domain";

import QueuelessTasksController from "./QueuelessTasksController";

const DEFAULT_QUEUE_NAME = `${dama_host_id}:DEFAULT_QUEUE`;

const ts_node_path = join(__dirname, "../../../node_modules/.bin/ts-node");
const task_runner_path = join(__dirname, "./TaskRunner.ts");

// This class should be used in the main DamaController server to queue and execute DamaTasks.
export default class TasksControllerWithWorkers extends QueuelessTasksController {
  private pgboss_worker_configs_by_queue_name: Record<
    DamaTaskQueueName,
    PgBossWorkOptions
  >;

  private pgboss_worker_id_by_queue_name_by_pgenv: Record<
    PgEnv,
    Record<DamaTaskQueueName, Promise<PgBossWorker["id"] | undefined>>
  >;

  constructor() {
    super();
    this.pgboss_worker_configs_by_queue_name = {};
    this.pgboss_worker_id_by_queue_name_by_pgenv = {};

    this.registerTaskQueue(DEFAULT_QUEUE_NAME, {});
  }

  private async queueExistsOnPgEnv(
    dama_task_queue_name: DamaTaskQueueName,
    pg_env = this.pg_env
  ) {
    dama_task_queue_name =
      this.prefixDamaTaskQueueNameWithHostId(dama_task_queue_name);

    let db: NodePgPoolClient;

    try {
      db = await dama_db.getDbConnection(pg_env);

      const sql = dedent(`
        SELECT EXISTS (
          SELECT
              1
            FROM data_manager.dama_task_queue
            WHERE ( task_queue_name = $1 )
        ) AS queue_exists;
      `);

      const {
        rows: [{ queue_exists }],
      } = await db.query({ text: sql, values: [dama_task_queue_name] });

      return queue_exists;
    } catch (err) {
      console.error(err);
      return false;
    } finally {
      // eslint-disable-next-line
      process.nextTick(() => db?.release());
    }
  }

  private getTaskQueueWorkerOptions(dama_task_queue_name: DamaTaskQueueName) {
    dama_task_queue_name =
      this.prefixDamaTaskQueueNameWithHostId(dama_task_queue_name);

    return (
      this.pgboss_worker_configs_by_queue_name[dama_task_queue_name] || null
    );
  }

  async registerTaskQueue(
    dama_task_queue_name: DamaTaskQueueName,
    options: PgBossWorkOptions = {}
  ) {
    dama_task_queue_name =
      this.prefixDamaTaskQueueNameWithHostId(dama_task_queue_name);

    const orig_opts = this.getTaskQueueWorkerOptions(dama_task_queue_name);

    if (orig_opts) {
      if (!_.isEqual(options, orig_opts)) {
        throw new Error(
          "TaskQueue has already been registered with different PgBossWorkOptions."
        );
      }

      return;
    }

    this.pgboss_worker_configs_by_queue_name[dama_task_queue_name] =
      _.cloneDeep(options);

    // await this.restartQueueWorkersAcrossPgEnvs(dama_task_queue_name);

    return;
  }

  async restartQueueWorkersAcrossPgEnvs(
    dama_task_queue_name: DamaTaskQueueName
  ) {
    const prefixed_dama_task_queue_name =
      this.prefixDamaTaskQueueNameWithHostId(dama_task_queue_name);

    const options = this.getTaskQueueWorkerOptions(
      prefixed_dama_task_queue_name
    );

    if (!options) {
      throw new Error(
        `You must first call TasksController.registerTaskQueue(${dama_task_queue_name}).`
      );
    }

    const all_pg_envs = await dama_db.listAllPgEnvs();

    await Promise.all(
      all_pg_envs.map(async (pg_env) => {
        const queue_exists = await this.queueExistsOnPgEnv(
          prefixed_dama_task_queue_name,
          pg_env
        );

        if (queue_exists) {
          await this.startDamaQueueWorker(
            pg_env,
            prefixed_dama_task_queue_name
          );
        }
      })
    );
  }

  private async startDamaQueueWorker(
    pg_env: PgEnv,
    dama_task_queue_name: DamaTaskQueueName
  ) {
    const prefixed_dama_task_queue_name =
      this.prefixDamaTaskQueueNameWithHostId(dama_task_queue_name);

    const existing_worker_p =
      this.pgboss_worker_id_by_queue_name_by_pgenv[pg_env]?.[
        prefixed_dama_task_queue_name
      ];

    if (existing_worker_p) {
      return await existing_worker_p;
    }

    const options = this.getTaskQueueWorkerOptions(
      prefixed_dama_task_queue_name
    );

    if (!options) {
      throw new Error(
        `DamaTaskQueue ${dama_task_queue_name} is not registered.`
      );
    }

    this.pgboss_worker_id_by_queue_name_by_pgenv[pg_env] =
      this.pgboss_worker_id_by_queue_name_by_pgenv[pg_env] || {};

    let done: (info: PgBossWorker["id"]) => PgBossWorker["id"];
    let fail: Function;

    this.pgboss_worker_id_by_queue_name_by_pgenv[pg_env][
      prefixed_dama_task_queue_name
    ] = new Promise((resolve, reject) => {
      // @ts-ignore
      done = resolve;
      fail = reject;
    });

    try {
      // FIXME: Going to have to runInDamaContexts for each pg_env in config
      const boss = await this.getPgBoss(pg_env);

      // boss.work returns a UUID.
      const id = await boss.work(
        prefixed_dama_task_queue_name,
        options,
        this.startDamaTask.bind(this, pg_env)
      );

      process.nextTick(() => done(id));

      console.log("started worker for", prefixed_dama_task_queue_name);

      return id;
    } catch (err) {
      process.nextTick(() => fail(err));
      throw err;
    }
  }

  private async startDamaTask(pg_env: PgEnv, dama_job: DamaTaskJob) {
    const {
      data: { etl_context_id },
    } = dama_job;

    const AVAIL_DAMA_ETL_CONTEXT_ID = `${etl_context_id}`;

    // When running in debug mode, the Task processes stdout/stderr
    // will be piped to the the  TasksController's stdout/stderr.
    const run_in_debug_mode = true;
    // const run_in_debug_mode =
    // process.env.AVAIL_DAMA_TASKS_DEBUG_MODE === "1" ||
    // process.env.AVAIL_DAMA_TASKS_DEBUG_MODE === "true";

    const detached = run_in_debug_mode;
    const stdio = run_in_debug_mode ? "inherit" : "ignore";

    try {
      await execa(ts_node_path, [task_runner_path], {
        env: {
          ...process.env,
          AVAIL_DAMA_PG_ENV: this.pg_env,
          AVAIL_DAMA_ETL_CONTEXT_ID,
        },

        detached,
        stdio,
      });

      return;
    } catch (err) {
      console.error(err);
      console.error(`ExecaErrorCode: ${(<ExecaError>err).exitCode}`);

      // @ts-ignore
      switch (err.exitCode) {
        case DamaTaskExitCodes.COULD_NOT_ACQUIRE_INITIAL_EVENT_LOCK:
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
    const db = await dama_db.getDbConnection(pg_env);

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

    db.release();

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

    const db = await dama_db.getDbConnection(pg_env);

    const sql = dedent(`
      SELECT
          etl_status
        FROM data_manager.etl_contexts
        WHERE ( etl_context_id = $1 )
    `);

    const {
      rows: [{ etl_status }],
    } = await db.query(sql, [etl_context_id]);

    db.release();

    console.log("==> EtlContext", etl_context_id, "etl_status =", etl_status);

    if (etl_status !== "DONE") {
      throw new Error("DamaTask failed.");
    }

    return null;
  }

  async queueDamaTask(
    dama_task_descr: DamaTaskDescriptor,
    pgboss_send_options: PgBossSendOptions,
    pg_env = this.pg_env
  ) {
    const { dama_task_queue_name = DEFAULT_QUEUE_NAME } = dama_task_descr;

    const prefixed_dama_task_queue_name =
      this.prefixDamaTaskQueueNameWithHostId(dama_task_queue_name);

    if (!this.getTaskQueueWorkerOptions(dama_task_queue_name)) {
      if (prefixed_dama_task_queue_name === DEFAULT_QUEUE_NAME) {
        await this.registerTaskQueue(prefixed_dama_task_queue_name);
      } else {
        throw new Error(
          "Non-default DataManager TaskQueues MUST be registered before tasks can be queued."
        );
      }
    }

    await this.startDamaQueueWorker(pg_env, dama_task_queue_name);

    return super.queueDamaTask(dama_task_descr, pgboss_send_options, pg_env);
  }
}
