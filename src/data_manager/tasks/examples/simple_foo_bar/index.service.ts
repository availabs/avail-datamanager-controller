import { join } from "path";

import { Context } from "moleculer";

import dama_events from "../../../events";

export const service_name = "data_manager/tasks/examples/simple_foo_bar";

const dama_task_queue_name = service_name;

const worker_path = join(__dirname, "./worker.ts");

export default {
  name: service_name,

  dependencies: ["dama/tasks"],

  actions: {
    startDamaTaskQueue: {
      visibility: "public",

      async handler(ctx: Context) {
        await ctx.call("dama/tasks.registerTaskQueue", {
          dama_task_queue_name,
          options: {
            teamSize: 10,
            teamConcurrency: 10,
            temRefil: true,
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
            payload: { delay: 5000, msg: "Hello, World!." },
          },
          worker_path,
        };

        const options = { retryLimit: 2, expireInSeconds: 30 };

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
