import { Context } from "moleculer";

import dama_meta from "data_manager/meta";
import dama_events from "data_manager/events";
import logger from "data_manager/logger";

import {
  npmrdsDataSourcesInitialMetadataByName,
  toposortedSourceNames,
  toposortedNpmrdsDataSourcesInitialMetadata,
} from "./domain";

export const serviceName = "dama/data_types/npmrds";

export default {
  name: serviceName,

  actions: {
    async getToposortedDamaSourcesMeta() {
      const metaByName: Record<string, object> =
        await dama_meta.getDamaSourceMetadataByName(toposortedSourceNames);

      const toposortedMeta = toposortedSourceNames.map(
        (name) =>
          // NOTE: if not in metaByName, will not have a source_id
          metaByName[name] || npmrdsDataSourcesInitialMetadataByName[name]
      );

      logger.silly(
        `==> ${serviceName} getToposortedDamaSourcesMeta ${JSON.stringify(
          toposortedMeta,
          null,
          4
        )}`
      );

      return toposortedMeta;
    },

    async initializeDamaSources(ctx: Context) {
      let toposortedDamaSrcMeta: any = await ctx.call(
        `${serviceName}.getToposortedDamaSourcesMeta`
      );

      if (
        toposortedDamaSrcMeta.every(({ source_id: id }) => Number.isFinite(id))
      ) {
        logger.info(
          `==> ${serviceName} initializeDamaSources: All NPMRDS DataSources already created.`
        );

        return toposortedDamaSrcMeta;
      }

      const etl_context_id = await dama_events.spawnEtlContext();

      const initialEvent = {
        type: `${serviceName}.initializeDamaSources:INITIAL`,
      };

      await dama_events.dispatch(initialEvent, etl_context_id);

      toposortedDamaSrcMeta = await dama_meta.loadToposortedDamaSourceMetadata(
        toposortedNpmrdsDataSourcesInitialMetadata
      );

      const final_event = {
        type: `${serviceName}.initializeDamaSources:FINAL`,
        payload: { toposortedDamaSrcMeta },
      };

      logger.info(
        `==> ${serviceName} initializeDamaSources: NPMRDS DataSources created.`
      );

      logger.debug(
        `==> ${serviceName} initializeDamaSources ${JSON.stringify(
          { final_event },
          null,
          4
        )}`
      );

      // @ts-ignore
      await dama_events.dispatch(final_event, etl_context_id);

      return toposortedDamaSrcMeta;
    },
  },
};
