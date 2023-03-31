import { Context } from "moleculer";

import TasksController from "./TasksController";

const dama_task_controller = new TasksController();

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

        await dama_task_controller.registerTaskQueue(
          dama_task_queue_name,
          options
        );
      },
    },

    startDamaQueueWorker: {
      visibility: "protected",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { dama_task_queue_name } = {},
          // @ts-ignore
          meta: { pgEnv },
        } = ctx;

        await dama_task_controller.startDamaQueueWorker(
          dama_task_queue_name,
          pgEnv
        );
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

        await dama_task_controller.queueDamaTask(dama_task_descr, options);
      },
    },

    getDamaTaskStatus: {
      visibility: "protected",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { etl_context_id },
          // @ts-ignore
          meta: { pgEnv },
        } = ctx;

        const status = await this.dama_task_controller.getDamaTaskStatus(
          pgEnv,
          etl_context_id
        );

        return status;
      },
    },
  },
};
