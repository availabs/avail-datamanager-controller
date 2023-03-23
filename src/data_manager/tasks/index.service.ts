import { Context } from "moleculer";

import AttachedTasksController from "./AttachedTasksController";

export default {
  name: "dama/tasks",

  actions: {
    registerTaskQueue: {
      visibility: "protected",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { dama_task_queue_name, options } = {},
          // @ts-ignore
          meta: { pgEnv },
        } = ctx;

        await this.dama_task_controller.registerTaskQueue(
          pgEnv,
          dama_task_queue_name
          // options
        );
      },
    },

    queueDamaTask: {
      visibility: "protected",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { dama_task_descr, options },
          // @ts-ignore
          meta: { pgEnv },
        } = ctx;

        console.log(JSON.stringify({ options }, null, 4));

        await this.dama_task_controller.queueDamaTask(
          pgEnv,
          dama_task_descr,
          options
        );
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

  created() {
    this.dama_task_controller = new AttachedTasksController(this);
  },
};
