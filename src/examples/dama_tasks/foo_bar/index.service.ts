import { join } from "path";

import { Context } from "moleculer";

export const serviceName = "examples/dama_tasks/foo_bar";

const worker_path = join(__dirname, "./worker.js");

const dama_task_queue_name = serviceName;

export default {
  name: serviceName,

  dependencies: ["dama/tasks"],

  actions: {
    sendIt: {
      visibility: "public",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          meta: { pgEnv },
        } = ctx;

        const dama_task_descr = {
          dama_task_queue_name,
          initial_event: {
            type: ":INITIAL",
            payload: { delay: 5000, msg: "Hello, World!." },
          },
          worker_path,
        };

        await ctx.call("dama/tasks.registerTaskQueue", {
          dama_task_queue_name,
          // options: {
          // teamSize: 10,
          // teamConcurrency: 10,
          // temRefil: true,
          // batchSize: 3,
          // },
        });

        const options = { retryLimit: 10, expireInSeconds: 30 };

        await ctx.call("dama/tasks.queueDamaTask", {
          dama_task_descr,
          options,
        });
      },
    },
  },
};
