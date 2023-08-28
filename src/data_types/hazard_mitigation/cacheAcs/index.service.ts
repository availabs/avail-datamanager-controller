import { join } from "path";
import dama_host_id from "constants/damaHostId";

import serviceName from "../constants/serviceName";

export default {
  name: `${serviceName}.cacheAcs`,
  actions: {
    load: {
      visibility: "published",

      async handler(ctx: any) {
        const {
          params: {
            source_id,
          },
        } = ctx;

        const worker_path = join(__dirname, "./load.worker.ts");

        const dama_task_descr = {
          worker_path,
          parent_context_id: null,
          source_id,
          initial_event: {
            type: "CACHE_ACS:INITIAL",
            payload: ctx?.params,
            meta: {
              __dama_task_manager__: {
                dama_host_id,
                worker_path,
              },
            },
          },
        };

        const options = { retryLimit: 0, expireInSeconds: 30000 };

        const { etl_context_id } = await ctx.call("dama/tasks.queueDamaTask", {
          dama_task_descr,
          options,
        });

        return { etl_context_id, source_id };
      },
    },
  },
};
