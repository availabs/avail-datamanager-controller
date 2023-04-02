import { join } from "path";

import { Context } from "moleculer";
import { FSA } from "flux-standard-action";

import dama_events from "../../../events";

export const service_name = "data_manager/tasks/examples/simple_foo_bar";

const dama_task_queue_name = service_name;

const worker_path = join(__dirname, "./worker.ts");

// AVAIL_LOGGING_LEVEL=silly AVAIL_DAMA_PG_ENV=dama_dev_1 AVAIL_DAMA_ETL_CONTEXT_ID=495 node --require ts-node/register src/data_manager/tasks/TaskRunner.ts

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

    // mol $ call data_manager/tasks/examples/simple_foo_bar.runWorkerOutsideDamaQueue --#pgEnv dama_dev_1 --#etl_context_id 496
    runWorkerOutsideDamaQueue: {
      visibility: "public",

      async handler() {
        console.warn(
          "WARNING: runWorkerOutsideDamaQueue bypasses idempotency checks and therefore should ONLY be used in development."
        );
        const initial_event = await dama_events.getInitialEvent();

        let final_event: FSA | null = null;

        try {
          final_event = await dama_events.getEtlContextFinalEvent();
        } catch (err) {
          //
        }

        try {
          // NOTE: no need for runInDamaContext because this Action is within the EtlContext.
          const {
            default: main,
          }: {
            default: (initial_event: FSA) => FSA | Promise<FSA> | unknown;
          } = await import(worker_path);

          final_event = <FSA>await main(initial_event);

          await dama_events.dispatch(<FSA>final_event);
        } catch (err) {
          const payload = {
            // @ts-ignore
            err_msg: err.message,
            timestamp: new Date().toISOString(),
          };

          // @ts-ignore
          dama_events.dispatch({ type: ":ERROR", payload, error: true });
        }
      },
    },
  },
};
