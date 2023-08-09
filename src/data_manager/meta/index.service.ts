import { Context as MoleculerContext } from "moleculer";

import dama_meta from ".";

export const serviceName = "dama/metadata";

export default {
  name: serviceName,

  actions: {
    getTableColumns: {
      visibility: "public",

      handler(ctx: MoleculerContext) {
        const {
          // @ts-ignore
          params: { tableSchema, tableName },
        } = ctx;

        return dama_meta.getTableColumns(tableSchema, tableName);
      },
    },

    insertNewRow: {
      visibility: "public",

      handler(ctx: MoleculerContext) {
        const {
          // @ts-ignore
          params: { tableSchema, tableName, newRow },
        } = ctx;

        return dama_meta.insertNewRow(tableSchema, tableName, newRow);
      },
    },

    getDamaSourceIdForName: {
      visibility: "public",

      handler(ctx: MoleculerContext) {
        const {
          // @ts-ignore
          params: { damaSourceName },
        } = ctx;

        return dama_meta.getDamaSourceIdForName(damaSourceName);
      },
    },

    getDataSourceLatestViewTableColumns: {
      visibility: "published",

      handler(ctx: MoleculerContext) {
        const {
          // @ts-ignore
          params: { damaSourceId },
        } = ctx;

        return dama_meta.getDataSourceLatestViewTableColumns(damaSourceId);
      },
    },

    getTableJsonSchema: {
      visibility: "public",

      handler(ctx: MoleculerContext) {
        // @ts-ignore
        const { tableSchema, tableName } = ctx.params;

        return dama_meta.getTableJsonSchema(tableSchema, tableName);
      },
    },

    getDamaDataSources: {
      visibility: "published",

      handler() {
        return dama_meta.getDamaDataSources();
      },
    },

    getDamaViewProperties(ctx: MoleculerContext) {
      const {
        // @ts-ignore
        params: { damaViewId, properties },
      } = ctx;

      return dama_meta.getDamaViewProperties(damaViewId, properties);
    },

    getDamaViewName(ctx: MoleculerContext) {
      const {
        // @ts-ignore
        params: { damaViewId },
      } = ctx;

      return dama_meta.getDamaViewName(damaViewId);
    },

    getDamaViewNamePrefix(ctx: MoleculerContext) {
      const {
        // @ts-ignore
        params: { damaViewId },
      } = ctx;

      return dama_meta.getDamaViewNamePrefix(damaViewId);
    },

    getDamaViewGlobalId(ctx: MoleculerContext) {
      const {
        // @ts-ignore
        params: { damaViewId },
      } = ctx;

      return dama_meta.getDamaViewGlobalId(damaViewId);
    },

    getDamaViewMapboxPaintStyle(ctx: MoleculerContext) {
      const {
        // @ts-ignore
        params: { damaViewId },
      } = ctx;

      return dama_meta.getDamaViewMapboxPaintStyle(damaViewId);
    },

    createNewDamaSource: {
      visibility: "published",

      handler(ctx: MoleculerContext) {
        // @ts-ignore
        return dama_meta.createNewDamaSource(ctx.params);
      },
    },

    createNewDamaView: {
      visibility: "published",

      async handler(ctx: MoleculerContext) {
        // @ts-ignore
        const { params }: { params: object } = ctx;

        // @ts-ignore
        let { etl_context_id } = params;

        if (!etl_context_id) {
          // @ts-ignore
          etl_context_id = ctx.meta.etl_context_id || null;
        }

        const newRow = {
          ...params,
          etl_context_id,
        };

        return dama_meta.createNewDamaView(newRow);
      },
    },

    deleteDamaView: {
      visibility: "published",

      handler(ctx: MoleculerContext) {
        const {
          // @ts-ignore
          params: { view_id },
        } = ctx;

        return dama_meta.deleteDamaView(view_id);
      },
    },

    deleteDamaSource: {
      // remove a view entry. ideally should keep track of deletes with userids, and etl ids.
      visibility: "published",

      handler(ctx: MoleculerContext) {
        const {
          // @ts-ignore
          params: { source_id },
        } = ctx;

        return dama_meta.deleteDamaSource(source_id);
      },
    },

    makeAuthoritativeDamaView: {
      // update view meta, update other views of the source too.
      handler(ctx: MoleculerContext) {
        const {
          // @ts-ignore
          params: { view_id },
        } = ctx;

        return dama_meta.makeAuthoritativeDamaView(view_id);
      },
    },

    getDamaSourceMetadataByName(ctx: MoleculerContext) {
      const {
        // @ts-ignore
        params: { damaSourceNames },
      } = ctx;

      return dama_meta.getDamaSourceMetadataByName(damaSourceNames);
    },

    loadToposortedDamaSourceMetadata(ctx: MoleculerContext) {
      const {
        // @ts-ignore
        params: { toposortedDataSourcesMetadata },
      } = ctx;

      return dama_meta.loadToposortedDamaSourceMetadata(
        toposortedDataSourcesMetadata
      );
    },

    getMBTilesMetadataForView: {
      visibility: "published",

      async handler(ctx: MoleculerContext) {
        const {
          // @ts-ignore
          params: { view_id },
        } = ctx;

        const mbtiles_metadata = await dama_meta.getMBTilesMetadataForView(
          view_id
        );

        if (mbtiles_metadata === null) {
          throw new Error(`No MBTiles metadata for DamaViewID ${view_id}.`);
        }

        return mbtiles_metadata;
      },
    },

    getCurrentActiveViewsForDamaSourceName: {
      visibility: "published",

      async handler(ctx: MoleculerContext) {
        const {
          // @ts-ignore
          params: { source_name },
        } = ctx;

        const active_views =
          await dama_meta.getCurrentActiveViewsForDamaSourceName(source_name);

        if (active_views === null) {
          throw new Error(
            `No currently active DamaViews for DamaSourceName ${source_name}.`
          );
        }

        return active_views;
      },
    },
  },
};
