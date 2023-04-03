import { join } from "path";

import { Context } from "moleculer";

import dama_events from "../../../events";

export const service_name =
  "data_manager/dama_tasks/examples/chaotic_concurrent_subtasks_fizzbuzz";

const dama_task_queue_name = service_name;

const worker_path = join(__dirname, "./worker.ts");

// AVAIL_LOGGING_LEVEL=silly AVAIL_DAMA_PG_ENV=dama_dev_1 AVAIL_DAMA_ETL_CONTEXT_ID=495 node --require ts-node/register src/data_manager/tasks/TaskRunner.ts

export default {
  name: service_name,

  dependencies: ["dama/tasks"],

  actions: {
    startTaskQueue: {
      visibility: "public",

      async handler(ctx: Context) {
        await ctx.call("dama/tasks.registerTaskQueue", {
          dama_task_queue_name,
          options: {
            teamSize: 10,
            teamConcurrency: 10,
            teamRefill: true,
            // batchSize: 3,
          },
        });

        await ctx.call("dama/tasks.startDamaQueueWorker", {
          dama_task_queue_name,
        });
      },
    },

    sendIt: {
      visibility: "public",

      async handler(ctx: Context) {
        const dama_task_descr = {
          dama_task_queue_name,
          initial_event: {
            type: ":INITIAL",
            payload: {
              n: 1,
              iterations: 30,
              chaos_factor: Math.random() * 0.1,
            },
          },
          worker_path,
        };

        const options = { retryLimit: 1e6, expireInHours: 24 };

        // @ts-ignore
        const { etl_context_id } = await ctx.call("dama/tasks.queueDamaTask", {
          dama_task_descr,
          options,
        });

        dama_events.registerEtlContextFinalEventListener(
          etl_context_id,
          (final_event) => {
            console.log("===== Dama Task Done =====");
            console.log(JSON.stringify({ final_event }, null, 4));
            console.log("==========================");
          }
        );
      },
    },
  },
};
