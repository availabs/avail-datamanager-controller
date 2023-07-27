import { join } from "path";
import dama_host_id from "constants/damaHostId";
import dama_meta from "data_manager/meta";
import serviceName from "../constants/serviceName";

export default {
  name: `${serviceName}.tigerFullDownloadAction`,
  actions: {
    load: {
      visibility: "published",

      async handler(ctx: any) {
        let source: Record<string, any>;
        let source_id: number | null = ctx?.params?.source_id;
        const type = "tl_full";

        if (!source_id) {
          source = await dama_meta.createNewDamaSource({
            name: ctx?.params?.source_name,
            type,
          });

          source_id = source?.source_id;
          ctx.params.source_id = source_id;
        }

        const worker_path = join(__dirname, "./load.worker.ts");

        const dama_task_descr = {
          worker_path,
          parent_context_id: null,
          source_id,
          initial_event: {
            type: "Tiger_Full_Dataset:INITIAL",
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
