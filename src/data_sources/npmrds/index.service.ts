import { Context } from "moleculer";

import { toposortedNpmrdsDataSourcesInitialMetadata } from "./domain";

export const serviceName = "dama/data_sources/npmrds";

export default {
  name: serviceName,

  actions: {
    initializeDamaSources: {
      visibility: "public",
      async handler(ctx: Context) {
        const toposortedDamaSrcMeta = await ctx.call(
          "dama_db.loadToposortedDamaSourceMetadata",
          {
            toposortedDataSourcesMetadata:
              toposortedNpmrdsDataSourcesInitialMetadata,
          }
        );

        return toposortedDamaSrcMeta;
      },
    },
  },
};
