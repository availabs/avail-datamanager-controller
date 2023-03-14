import {
  readdir as readdirAsync,
  stat as statAsync,
  readFile as readFileAsync,
} from "fs/promises";
import { join } from "path";

import execa from "execa";
import PgBoss from "pg-boss";

import _ from "lodash";

import dedent from "dedent";
import pgFormat from "pg-format";

import damaHost from "../../../../src/constants/damaHost";

import {
  PgEnv,
  NodePgClient,
  getConnectedNodePgClient,
  getPostgresConnectionUri,
} from "../../../../src/data_manager/dama_db/postgres/PostgreSQL";

import { DamaTaskDescriptor, DamaTaskJob, DamaTaskExitCodes } from "./types";

const queue_name = `${damaHost}:queue`;

export default class TasksController {
  private db!: NodePgClient;
  public ready: Promise<void>;

  private boss: PgBoss;

  constructor(readonly pg_env: PgEnv) {
    this.ready = this.initialize();
  }

  private async initialize() {
    this.db = await getConnectedNodePgClient(this.pg_env);

    await this.db.query("CREATE EXTENSION IF NOT EXISTS pgcrypto;");

    const pgConnectionUri = getPostgresConnectionUri(this.pg_env);

    this.boss = new PgBoss(pgConnectionUri);

    //  Note: This will create the pgboss SCHEMA an its TABLES
    //        See https://github.com/timgit/pg-boss/blob/master/docs/readme.md#start
    await this.boss.start();

    //  CREATE the data_manager.dama_task_queue VIEW
    //    that JOINs the pgboss TABLEs with the etl_contexts TABLE.
    const fpath = join(__dirname, "./sql/create_dama_pgboss_view.sql");
    const sql = await readFileAsync(fpath, { encoding: "utf8" });

    await this.db.query(sql);
  }

  async startTaskQueue() {
    await this.ready;

    await this.boss.work(
      queue_name,
      {
        teamSize: 3,
        teamConcurrency: 3,
        teamRefill: true,
      },
      this.startDamaTask.bind(this)
    );
  }

  //  This method was used to implement a now deprecated way of preventing
  //  duplicate running tasks. That logic was replaced with the :INITIAL event
  //  lock. The output of this method may be useful if we eventually want to
  //  implement killing a running task via the UI.
  async getDamaTasksRunningOnHost() {
    await this.ready;

    const allPids = (await readdirAsync("/proc/")).filter((name) =>
      /^[0-9]+$/.test(name)
    );

    const uid = process.getuid();

    const running_dama_tasks = (
      await Promise.all(
        allPids.map(async (pid) => {
          try {
            const stats = await statAsync(join("/proc", pid));

            if (stats.uid !== uid) {
              return null;
            }

            const fpath = join("/proc/", pid, "environ");

            const environ = (await readFileAsync(fpath, { encoding: "utf8" }))
              .split("\0")
              .reduce((acc, line) => {
                if (!/^AVAIL_DAMA/.test(line)) {
                  return acc;
                }

                const [k, v] = line.split("=");
                acc[k] = v;
                return acc;
              }, {});

            return environ["AVAIL_DAMA_ETL_CONTEXT_ID"]
              ? { pid, environ }
              : null;
          } catch (err) {
            return null;
          }
        })
      )
    )
      .filter(Boolean)
      .sort(
        // @ts-ignore
        (a, b) => +a.AVAIL_DAMA_ETL_CONTEXT_ID - +b.AVAIL_DAMA_ETL_CONTEXT_ID
      );

    return running_dama_tasks;
  }

  private async startDamaTask(dama_job: DamaTaskJob) {
    await this.ready;

    const {
      id: AVAIL_DAMA_TASK_ID,
      data: { etl_context_id = null, worker_path },
    } = dama_job;

    console.log("DamaTask  for EtlContext", etl_context_id, "STARTED");

    const AVAIL_DAMA_ETL_CONTEXT_ID = `${etl_context_id}`;

    const run_in_debug_mode =
      process.env.AVAIL_DAMA_TASKS_DEBUG_MODE === "1" ||
      process.env.AVAIL_DAMA_TASKS_DEBUG_MODE === "true";

    const detached = run_in_debug_mode;
    const stdio = run_in_debug_mode ? "inherit" : "ignore";

    try {
      const task_process = execa.node(worker_path, {
        env: {
          ...process.env,
          AVAIL_DAMA_PG_ENV: this.pg_env,
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
      console.error(
        err.exitCode,
        DamaTaskExitCodes.COULD_NOT_ACQUIRE_INITIAL_EVENT_LOCK
      );

      switch (err.exitCode) {
        case DamaTaskExitCodes.TASK_ALREADY_DONE:
          console.log("==> TASK_ALREADY_DONE");
          return null;
        case DamaTaskExitCodes.COULD_NOT_ACQUIRE_INITIAL_EVENT_LOCK:
          console.log("==> COULD_NOT_ACQUIRE_INITIAL_EVENT_LOCK");
          return await this.handleDuplicateTask(etl_context_id);
        default:
          throw err;
      }
    }
  }

  private async damaTaskInitialEventLockIsLocked(etl_context_id: number) {
    await this.ready;

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
    ] = await this.db.query(sql);

    return etl_context_exists && unable_to_select_initial_event;
  }

  async handleDuplicateTask(etl_context_id: number) {
    await this.ready;

    // poll the :INITIAL event lock until the running DamaTask releases it.
    while (await this.damaTaskInitialEventLockIsLocked(etl_context_id)) {
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }

    const sql = dedent(`
      SELECT
          etl_status
        FROM data_manager.etl_contexts
        WHERE ( etl_context_id = $1 )
    `);

    const {
      rows: [{ etl_status }],
    } = await this.db.query(sql, [etl_context_id]);

    console.log("==> EtlContext", etl_context_id, "etl_status =", etl_status);

    if (etl_status === "DONE") {
      return null;
    }

    throw new Error("DamaTask failed.");
  }

  async queueDamaTask(dama_task_descr: DamaTaskDescriptor) {
    await this.ready;

    const {
      parent_context_id = null,
      source_id = null,
      initial_event,
      worker_path,
    } = dama_task_descr;

    if (!/:INITIAL$/.test(initial_event.type)) {
      throw new Error(
        "DamaTask Invariant Violation: initial_event MUST be an :INITIAL event."
      );
    }

    try {
      await this.db.query("BEGIN ;");

      const {
        rows: [{ etl_context_id }],
      } = await this.db.query({
        name: "QUEUE_DAMA_TASK",
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

      const task_meta = { ...meta, dama_host_id: damaHost };

      await this.db.query({
        name: "INSERT_DAMA_TASK_INITIAL_EVENT",
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

      await this.db.query("COMMIT ;");

      const etl_task_id = await this.boss.send(
        queue_name,
        {
          etl_context_id,
          worker_path,
        },
        { retryLimit: 10, expireInSeconds: 5 }
      );

      if (etl_task_id) {
        await this.db.query({
          name: "UPDATE_ETL_CONTEXT_TASK_ID",
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
      await this.db.query("ROLLACK ;");
    }
  }
}
