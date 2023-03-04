import { Context } from "moleculer";

import {
  npmrdsDataSourcesInitialMetadataByName,
  toposortedSourceNames,
  toposortedNpmrdsDataSourcesInitialMetadata,
} from "./domain";

export const serviceName = "dama/data_types/npmrds";

export default {
  name: serviceName,

  actions: {
    async getToposortedDamaSourcesMeta(ctx: Context) {
      const metaByName: Record<string, object> = await ctx.call(
        "dama/metadata.getDamaSourceMetadataByName",
        { damaSourceNames: toposortedSourceNames }
      );

      const toposortedMeta = toposortedSourceNames.map(
        (name) =>
          // NOTE: if not in metaByName, will not have a source_id
          metaByName[name] || npmrdsDataSourcesInitialMetadataByName[name]
      );

      return toposortedMeta;
    },

    async initializeDamaSources(ctx: Context) {
      let toposortedDamaSrcMeta: any = await ctx.call(
        `${serviceName}.getToposortedDamaSourcesMeta`
      );

      // @ts-ignore
      if (
        toposortedDamaSrcMeta.every(({ source_id: id }) => Number.isFinite(id))
      ) {
        return toposortedDamaSrcMeta;
      }

      const etl_context_id = await ctx.call(
        "data_manager/events.spawnEtlContext"
      );

      const initialEvent = {
        type: `${serviceName}.initializeDamaSources:INITIAL`,
      };

      await ctx.call("data_manager/events.dispatch", initialEvent, {
        parentCtx: ctx,
        meta: { etl_context_id },
      });

      toposortedDamaSrcMeta = await ctx.call(
        "dama/metadata.loadToposortedDamaSourceMetadata",
        {
          toposortedDataSourcesMetadata:
            toposortedNpmrdsDataSourcesInitialMetadata,
        }
      );

      // NOTE: This complicates visualizing NpmrdsTravelTimesExportRitis requests.
      // const [{ source_id }] = toposortedDamaSrcMeta;

      // await ctx.call("data_manager/events.setEtlContextSourceId", {
      //   etl_context_id,
      //   source_id,
      // });

      const finalEvent = {
        type: `${serviceName}.initializeDamaSources:FINAL`,
        payload: { toposortedDamaSrcMeta },
      };

      await ctx.call("data_manager/events.dispatch", finalEvent, {
        parentCtx: ctx,
        meta: { etl_context_id },
      });

      return toposortedDamaSrcMeta;
    },
  },
};
