import { join } from "path";

import { Context as MoleculerContext } from "moleculer";

import dama_tasks from "data_manager/tasks";

import { TaskQueueConfigs } from "./queues";

import { TaskQueue } from "./domain";

import {
  getToposortedDamaSourcesMeta,
  initializeDamaSources,
} from "./utils/dama_sources";

import { InitialEvent } from ".";

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

    queueNpmrdsAggregateEtl: {
      visibility: "public",

      async handler(ctx: MoleculerContext) {
        const {
          // @ts-ignore
          params: { state, start_date, end_date, is_expanded },
        } = ctx;

        const initial_event: InitialEvent = {
          type: ":INITIAL",
          payload: {
            state,
            start_date,
            end_date,
            is_expanded,
          },
          meta: { note: "NPMRDS aggregate ETL" },
        };

        const dama_task_descr = {
          worker_path: join(__dirname, "./worker.ts"),

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
  },
};
