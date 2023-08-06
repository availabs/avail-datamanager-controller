import { Context } from "moleculer";

import dama_gis from ".";

import serviceName from "./constants/serviceName";

export default {
  name: serviceName,

  actions: {
    getDamaGisDatasetViewTableSchemaSummary(ctx: Context) {
      const {
        // @ts-ignore
        params: { damaViewId },
      } = ctx;

      return dama_gis.getDamaGisDatasetViewTableSchemaSummary(damaViewId);
    },

    async generateGisDatasetViewGeoJsonSqlQuery(ctx: Context) {
      const {
        // @ts-ignore
        params: { damaViewId, config = {} },
      } = ctx;

      return dama_gis.generateGisDatasetViewGeoJsonSqlQuery(damaViewId, config);
    },

    makeDamaGisDatasetViewGeoJsonFeatureAsyncIterator: {
      visibility: "protected",

      // https://node-postgres.com/features/queries#query-config-object
      async *handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { damaViewId, config = {} },
        } = ctx;

        return dama_gis.makeDamaGisDatasetViewGeoJsonFeatureAsyncIterator(
          damaViewId,
          config
        );
      },
    },

    makeDamaGisDatasetViewGeoJsonlStream: {
      visibility: "published",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { damaViewId, config = {} },
        } = ctx;

        return dama_gis.makeDamaGisDatasetViewGeoJsonlStream(
          damaViewId,
          config
        );
      },
    },

    createDamaGisDatasetViewMbtiles: {
      visibility: "published",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { damaViewId },
        } = ctx;

        return dama_gis.createDamaGisDatasetViewMbtiles(damaViewId);
      },
    },
  },
};
