import { join } from "path";

import { Context as MoleculerContext } from "moleculer";

import dama_tasks from "data_manager/tasks";

export const serviceName = "data_types/dt-transcom_events";

export const task_queue_name = "dt-transcom_events";
export const subtasks_queue_name = "dt-transcom_events:subtasks";

export default {
  name: serviceName,

  actions: {
    startTaskQueue: {
      visibility: "public",

      async handler() {
        await dama_tasks.registerTaskQueue(task_queue_name, {
          teamSize: 1,
          teamConcurrency: 1,
        });

        await dama_tasks.startDamaQueueWorker(task_queue_name);

        await dama_tasks.registerTaskQueue(subtasks_queue_name, {
          teamSize: 1,
          teamConcurrency: 1,
        });

        await dama_tasks.startDamaQueueWorker(subtasks_queue_name);
      },
    },

    downloadTranscomEvents: {
      visibility: "public",

      async handler(ctx: MoleculerContext) {
        const {
          // @ts-ignore
          params: { start_timestamp, end_timestamp },
        } = ctx;

        const dama_task_descr = {
          worker_path: join(__dirname, "./worker.ts"),

          dama_task_queue_name: task_queue_name,

          initial_event: {
            type: ":INITIAL",
            payload: {
              start_timestamp,
              end_timestamp,
            },
          },
        };

        const options = { retryLimit: 1, expireInHours: 10 };

        const etl_context_id = await dama_tasks.queueDamaTask(
          dama_task_descr,
          options
        );

        return { etl_context_id };
      },
    },

    scheduleTranscomEventsEtl: {
      visibility: "public",

      async handler(ctx: MoleculerContext) {
        const {
          // @ts-ignore
          params: { cron },
        } = ctx;

        const dama_task_descr = {
          worker_path: join(__dirname, "./worker.ts"),

          dama_task_queue_name: task_queue_name,

          cron,

          initial_event: {
            type: ":INITIAL",
          },
        };

        const options = { retryLimit: 1, expireInHours: 10 };

        await dama_tasks.scheduleDamaTask(dama_task_descr, options);

        console.log(`DONE: scheduleTranscomEventsEtl cron='${cron}'`);
      },
    },

    unscheduleTranscomEventsEtl: {
      visibility: "public",

      async handler() {
        await dama_tasks.unscheduleDamaTask(task_queue_name);
      },
    },
  },
};
