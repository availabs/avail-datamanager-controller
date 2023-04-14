import { join } from "path";

import { Context as MoleculerContext } from "moleculer";

import dama_host_id from "constants/damaHostId";

import dama_events, { EtlEvent } from "data_manager/events";
import { getPgEnv, runInDamaContext } from "data_manager/contexts";
import { getLoggerForContext } from "data_manager/logger";

export const service_name = "data_manager/tasks/examples/simple_foo_bar";

const dama_task_queue_name = service_name;

// AVAIL_LOGGING_LEVEL=silly AVAIL_DAMA_PG_ENV=dama_dev_1 AVAIL_DAMA_ETL_CONTEXT_ID=495 node --require ts-node/register src/data_manager/tasks/TaskRunner.ts

export default {
  name: service_name,

  dependencies: ["dama/tasks"],

  actions: {
    startTaskQueue: {
      visibility: "public",

      async handler(ctx: MoleculerContext) {
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

      async handler(ctx: MoleculerContext) {
        const {
          // @ts-ignore
          params: { with_ctx = 1 },
        } = ctx;

        const fname = `worker.${+with_ctx ? "with" : "without"}-context.ts`;

        const worker_path = join(__dirname, fname);

        const dama_task_descr = {
          dama_task_queue_name,
          worker_path,
          initial_event: {
            type: ":INITIAL",
            payload: { delay: 5000, msg: "Hello, World!." },
            meta: {
              // By adding this, we can later restart the Task using the TaskRunner.
              __dama_task_manager__: {
                dama_host_id,
                worker_path,
              },
            },
          },
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

    // mol $ call data_manager/tasks/examples/simple_foo_bar.runWorkerOutsideDamaQueue --#pgEnv dama_dev_1
    runWorkerOutsideDamaQueue: {
      visibility: "public",

      async handler(ctx: MoleculerContext) {
        try {
          const {
            // @ts-ignore
            params: { with_ctx = 1 },
          } = ctx;

          const fname = `worker.${with_ctx ? "with" : "without"}-context.ts`;

          const worker_path = join(__dirname, fname);

          const etl_context_id = await dama_events.spawnEtlContext();

          const pgEnv = getPgEnv();
          const logger = getLoggerForContext(etl_context_id);

          const etl_ctx = {
            logger,
            meta: {
              pgEnv,
              etl_context_id,
            },
          };

          await runInDamaContext(etl_ctx, async () => {
            const initial_event = {
              type: ":INITIAL",
              payload: { delay: 5000, msg: "Hello, World!." },
              meta: {
                // By adding this, we can later restart the Task using the TaskRunner.
                __dama_task_manager__: {
                  dama_host_id,
                  worker_path,
                },
              },
            };

            await dama_events.dispatch(initial_event);

            const {
              default: main,
            }: {
              default: (initial_event: EtlEvent) => Promise<EtlEvent>;
            } = await import(worker_path);

            const final_event = await main(initial_event);

            await dama_events.dispatch(final_event);

            logger.debug(
              `simple_foo_bar pg_env=${pgEnv} etl_context_id=${etl_context_id} DONE`
            );
          });
        } catch (err) {
          console.error(err);
        }
      },
    },
  },
};
