import { Context } from "moleculer";

import dama_tasks from ".";

export default {
  name: "dama/tasks",

  actions: {
    registerTaskQueue: {
      visibility: "protected",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { dama_task_queue_name, options } = {},
        } = ctx;

        await dama_tasks.registerTaskQueue(dama_task_queue_name, options);
      },
    },

    startDamaQueueWorker: {
      visibility: "protected",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { dama_task_queue_name } = {},
        } = ctx;

        await dama_tasks.startDamaQueueWorker(dama_task_queue_name);
      },
    },

    queueDamaTask: {
      visibility: "protected",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { dama_task_descr, options },
        } = ctx;

        console.log(JSON.stringify({ options }, null, 4));

        return dama_tasks.queueDamaTask(dama_task_descr, options);
      },
    },

    getDamaTaskStatus: {
      visibility: "protected",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { etl_context_id },
        } = ctx;

        const status = await dama_tasks.getDamaTaskStatus(etl_context_id);

        return status;
      },
    },
  },
};
