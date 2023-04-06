import { join } from "path";

import { Context as MolecularContext } from "moleculer";

export const serviceName = "data_types/dt-transcom_events";

import { InitialEvent } from "./tasks/transcom_events";

export default {
  name: serviceName,

  dependencies: ["dama/tasks"],

  actions: {
    async downloadTranscomEvents(ctx: MolecularContext) {
      const {
        params: { start_timestamp, end_timestamp },
        meta: { pgEnv },
      } = ctx;

      const dama_task_descr = {
        worker_path: join(__dirname, "./tasks/transcom_events/worker.ts"),

        dama_task_queue_name: join(serviceName, "download_transcom_events"),

        initial_event: {
          type: ":INITIAL",
          payload: {
            start_timestamp,
            end_timestamp,
          },
        },
      };

      const options = { retryLimit: 1, expireInHours: 12 };

      // @ts-ignore
      const { etl_context_id } = await ctx.call("dama/tasks.queueDamaTask", {
        dama_task_descr,
        options,
      });

      return { etl_context_id, pg_env: pgEnv };
    },
  },
};
