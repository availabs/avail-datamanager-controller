import { inspect } from "util";
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
import { table } from "table";

import dama_db from "../dama_db";
import dama_events from "../events";

import dama_host_id from "../../constants/damaHostId";

import { NodePgPoolClient, PgEnv } from "../dama_db/postgres/PostgreSQL";

import {
  DamaTaskQueueName,
  DamaTaskDescriptor,
  DamaTaskJob,
  DamaTaskExitCodes,
} from "./domain";

import BaseTasksController from "./BaseTasksController";

const node_js_path = process.env.NODE_JS_PATH || "node";

const DEFAULT_QUEUE_NAME = `${dama_host_id}:DEFAULT_QUEUE`;

const task_runner_path = join(__dirname, "./TaskRunner.ts");

// This class should be used in the main DamaController server to queue and execute DamaTasks.
export default class TasksControllerWithWorkers extends BaseTasksController {
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
    this.logger.silly(
      `registerTaskQueue: dama_task_queue_name=${dama_task_queue_name} options=${inspect(
        options
      )}`
    );

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
            prefixed_dama_task_queue_name,
            pg_env
          );
        }
      })
    );
  }

  async startDamaQueueWorker(
    dama_task_queue_name: DamaTaskQueueName,
    pg_env = this.pg_env
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
    let {
      // eslint-disable-next-line prefer-const
      id: etl_task_id,
      // @ts-ignore
      // eslint-disable-next-line prefer-const
      data: { worker_path, etl_context_id, initial_event, source_id },
    } = dama_job;

    this.logger.debug(`Starting DamaTask ${JSON.stringify(dama_job, null, 4)}`);

    //  When jobs are queued, the EtlContext is created and the initial_event is dispatched.
    //    dama_job.data.etl_context_id will be defined and the :INITIAL event is already in the database.
    //
    //  For scheduled jobs, we have create the EtlContext and dispatch the initial_event here.
    //    dama_job.data.etl_context_id will be undefined and
    //    dama_job.data.initial_event MUST be defined if the task needs configuration data.
    //
    if (!etl_context_id) {
      this.logger.debug("===== spawning EtlContext");
      etl_context_id = await dama_db.runInTransactionContext(async () => {
        const eci = await dama_events.spawnEtlContext(source_id, null, pg_env);

        initial_event = initial_event || {
          type: ":INTITIAL",
        };

        const { type = null, payload = null, meta = {} } = initial_event;

        const task_meta = {
          ...meta,
          __dama_task_manager__: {
            // The following two pieces of information are required by the TaskRunner.
            worker_path,
            dama_host_id,
          },
        };

        const task_initial_event = {
          type,
          payload,
          meta: task_meta,
        };

        // @ts-ignore
        await dama_events.dispatch(task_initial_event, eci, pg_env);

        await dama_db.query({
          text: dedent(`
            UPDATE data_manager.etl_contexts
              SET etl_task_id = $1
            WHERE ( etl_context_id = $2 )
          `),
          values: [etl_task_id, eci],
        });

        return eci;
      }, pg_env);

      this.logger.debug(
        `===== spawned EtlContext etl_context_id=${etl_context_id}`
      );
    }

    const AVAIL_DAMA_ETL_CONTEXT_ID = `${etl_context_id}`;

    try {
      this.logger.debug("execa");
      await execa(
        node_js_path,
        [
          // https://www.npmjs.com/package/tsconfig-paths#register
          "--require",
          "tsconfig-paths/register",
          "--require",
          "ts-node/register",
          task_runner_path,
        ],
        {
          env: {
            ...process.env,
            AVAIL_DAMA_PG_ENV: pg_env,
            AVAIL_DAMA_ETL_CONTEXT_ID,
          },
          detached: true,
          stdio: "ignore",
        }
      );

      return;
    } catch (err) {
      // @ts-ignore
      switch (err.exitCode) {
        case DamaTaskExitCodes.COULD_NOT_ACQUIRE_INITIAL_EVENT_LOCK:
          this.logger.info(
            `Duplicate task: PgBoss handler adopting orphaned task process for etl_context_id ${etl_context_id}.`
          );
          return await this.handleDuplicateTask(pg_env, etl_context_id);
        default:
          this.logger.error(
            `==> TaskController: etl_context_id=${etl_context_id}; ExecaErrorCode: ${
              (<ExecaError>err).exitCode
            }`
          );
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
    this.logger.debug(
      `TasksController handleDuplicateTask pg_env=${pg_env} etl_context_id=${etl_context_id} start`
    );

    let locked_msg_level = "debug";

    // poll the :INITIAL event lock until the running DamaTask releases it.
    while (
      await this.damaTaskInitialEventLockIsLocked(pg_env, etl_context_id)
    ) {
      this.logger[locked_msg_level](
        `TasksController handleDuplicateTask pg_env=${pg_env} etl_context_id=${etl_context_id} :INITIAL event is locked`
      );

      locked_msg_level = "silly";

      await new Promise((resolve) => setTimeout(resolve, 1000));
    }

    this.logger.debug(
      `TasksController handleDuplicateTask pg_env=${pg_env} etl_context_id=${etl_context_id} :INITIAL event is NOT locked`
    );

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

    if (etl_status !== "DONE") {
      const err_msg = `dama_tasks.handleDuplicateTask: FAILED Task for etl_context_id=${etl_context_id}.`;

      this.logger.error(err_msg);

      throw new Error(err_msg);
    }

    this.logger.debug(
      `dama_tasks.handleDuplicateTask: Task DONE etl_context_id=${etl_context_id}, etl_status =${etl_status}`
    );

    return null;
  }

  async startDefaultQueueWorkerOrWarnIfNonDefaultWorkerNotActive(
    dama_task_queue_name: string,
    task_meta: any,
    pg_env: PgEnv
  ) {
    const prefixed_dama_task_queue_name =
      this.prefixDamaTaskQueueNameWithHostId(dama_task_queue_name);

    if (prefixed_dama_task_queue_name === DEFAULT_QUEUE_NAME) {
      return await this.startDamaQueueWorker(dama_task_queue_name, pg_env);
    }

    if (!this.getTaskQueueWorkerOptions(dama_task_queue_name)) {
      const warning =
        "WARNING: Queued tasks will not start until the TaskQueue is registered and Workers started.";

      const t = table(
        [
          ["pg_env", "dama_task_queue_name", "etl_context_id"],
          [pg_env, dama_task_queue_name, task_meta?.etl_context_id],
        ],
        {
          header: {
            alignment: "center",
            content: "Task sent to unregistered queue",
          },
        }
      );

      const msg = `${warning}\n${t}`;
      this.logger.warn(msg);
    } else if (
      !this.pgboss_worker_id_by_queue_name_by_pgenv[pg_env]?.[
        prefixed_dama_task_queue_name
      ]
    ) {
      const msg = dedent(
        `
          WARNING: The queued task for etl_context_id ${this.etl_context_id} will not start until
                   the TaskQueue ${dama_task_queue_name} for pg_env ${pg_env} has not been started.
        `
      );

      this.logger.warn(msg);
    }
  }

  async queueDamaTask(
    dama_task_descr: DamaTaskDescriptor,
    pgboss_send_options: PgBossSendOptions,
    pg_env = this.pg_env
  ) {
    const task_meta = await super.queueDamaTask(
      dama_task_descr,
      pgboss_send_options,
      pg_env
    );

    const { dama_task_queue_name = DEFAULT_QUEUE_NAME } = dama_task_descr;

    await this.startDefaultQueueWorkerOrWarnIfNonDefaultWorkerNotActive(
      dama_task_queue_name,
      task_meta,
      pg_env
    );

    return task_meta;
  }

  async scheduleDamaTask(
    dama_task_descr: DamaTaskDescriptor,
    pgboss_send_options: PgBossSendOptions,
    pg_env = this.pg_env
  ) {
    const task_meta = await super.scheduleDamaTask(
      dama_task_descr,
      pgboss_send_options,
      pg_env
    );

    const { dama_task_queue_name = DEFAULT_QUEUE_NAME } = dama_task_descr;

    await this.startDefaultQueueWorkerOrWarnIfNonDefaultWorkerNotActive(
      dama_task_queue_name,
      task_meta,
      pg_env
    );

    return task_meta;
  }
}
