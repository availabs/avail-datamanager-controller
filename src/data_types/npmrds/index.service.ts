import { join } from "path";

import { Context as MoleculerContext } from "moleculer";

import dama_tasks from "data_manager/tasks";

import { TaskQueueConfigs } from "./queues";

import { TaskQueue } from "./domain";

import {
  getToposortedDamaSourcesMeta,
  initializeDamaSources,
} from "./utils/dama_sources";

import { InitialEvent as BatchedEtlInitialEvent } from "./tasks/batched-etl";

const batch_etl_worker_path = join(__dirname, "./tasks/batched-etl.worker.ts");
const scheduled_etl_worker_path = join(
  __dirname,
  "./tasks/scheduled-etl.worker.ts"
);

export const serviceName = "dama/data_types/npmrds";

export default {
  name: serviceName,

  actions: {
    getToposortedDamaSourcesMeta,

    initializeDamaSources,

    startTaskQueues: {
      visibility: "public",

      async handler() {
        const task_queue_names = Object.keys(TaskQueueConfigs);

        for (const name of task_queue_names) {
          const { worker_options } = TaskQueueConfigs[name];

          await dama_tasks.registerTaskQueue(name, worker_options);

          await dama_tasks.startDamaQueueWorker(name);
        }
      },
    },

    // NOTE: Queues a batch ETL worker, not aggregate.
    queueNpmrdsAggregateEtl: {
      visibility: "public",

      async handler(ctx: MoleculerContext) {
        const params: any = ctx.params;

        const {
          state,
          start_date,
          end_date,
          is_expanded,
          override_max_paritions,
        } = params;

        const initial_event: BatchedEtlInitialEvent = {
          type: ":INITIAL",
          payload: {
            state,
            start_date,
            end_date,
            is_expanded,
            override_max_paritions,
          },
          meta: { note: "NPMRDS aggregate ETL" },
        };

        const dama_task_descr = {
          worker_path: batch_etl_worker_path,
          dama_task_queue_name: TaskQueue.AGGREGATE_ETL,
          initial_event,
        };

        const options = { retryLimit: 1, expireInHours: 7 * 24 };

        const etl_context_id = await dama_tasks.queueDamaTask(
          dama_task_descr,
          options
        );

        return { etl_context_id };
      },
    },

    scheduleNpmrdsEtl: {
      visibility: "public",

      async handler(ctx: MoleculerContext) {
        const {
          // @ts-ignore
          params: { cron },
        } = ctx;

        const dama_task_descr = {
          worker_path: scheduled_etl_worker_path,
          dama_task_queue_name: TaskQueue.AGGREGATE_ETL,
          cron,
          initial_event: {
            type: ":INITIAL",
            meta: { note: "Scheduled NPMRDS ETL" },
          },
        };

        const options = { retryLimit: 1, expireInHours: 12 };

        await dama_tasks.scheduleDamaTask(dama_task_descr, options);
      },
    },
  },
};
