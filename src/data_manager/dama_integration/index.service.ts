// FIXME: Need subEtlContexts for each layer

import { existsSync } from "fs";
import { readdir as readdirAsync } from "fs/promises";
import { join } from "path";

import { Context } from "moleculer";
import _ from "lodash";

import etlDir from "../../constants/etlDir";

import GeospatialDatasetIntegrator from "../../../tasks/gis-data-integration/src/data_integrators/GeospatialDatasetIntegrator";

import serviceName from "./constants/serviceName";
import EventTypes from "./constants/EventTypes";

import uploadGeospatialDataset from "./actions/uploadGeospatialDataset";
import stageLayerData from "./actions/stageLayerData";
import publishGisDatasetLayer from "./actions/publishGisDatasetLayer";

export default {
  name: serviceName,

  actions: {
    async getExistingDatasetUploads() {
      const dirs = await readdirAsync(etlDir, {
        encoding: "utf8",
      });

      const ids = dirs.reduce((acc: string[], dirName: string) => {
        const workDirPath = join(etlDir, dirName);
        const path = join(workDirPath, "layerNameToId.json");

        if (existsSync(path)) {
          const id = GeospatialDatasetIntegrator.createId(workDirPath);
          acc.push(id);
        }

        return acc;
      }, []);

      return ids;
    },

    uploadGeospatialDataset,

    getGeospatialDatasetLayerNames(ctx: Context) {
      const {
        // @ts-ignore
        params: { id },
      } = ctx;

      const gdi = new GeospatialDatasetIntegrator(id);

      // @ts-ignore
      const layerNameToId = gdi.layerNameToId;
      const layerNames = Object.keys(layerNameToId);

      return layerNames;
    },

    async getTableDescriptor(ctx: Context) {
      const {
        // @ts-ignore
        params: { id, layerName },
      } = ctx;

      const gdi = new GeospatialDatasetIntegrator(id);

      const tableDescriptor = await gdi.getLayerTableDescriptor(layerName);

      return tableDescriptor;
    },

    async getLayerAnalysis(ctx: Context) {
      const {
        // @ts-ignore
        params: { id, layerName },
      } = ctx;

      const gdi = new GeospatialDatasetIntegrator(id);

      const layerAnalysis = await gdi.getGeoDatasetLayerAnalysis(layerName);

      return layerAnalysis;
    },

    async updateTableDescriptor(ctx: Context) {
      const {
        // @ts-ignore
        params,
      } = ctx;

      // @ts-ignore
      const { id } = params;

      const gdi = new GeospatialDatasetIntegrator(id);

      // @ts-ignore
      const migration_result = await gdi.persistLayerTableDescriptor(params);

      return migration_result;
    },

    stageLayerData,

    async approveQA(ctx: Context) {
      const {
        // @ts-ignore
        params: { etl_context_id, user_id },
      } = ctx;

      if (!(etl_context_id && user_id)) {
        throw new Error(
          "The etl_context_id and user_id parameters are required."
        );
      }

      const event = {
        type: EventTypes.QA_APPROVED,
        meta: {
          etl_context_id,
          user_id,
          timestamp: new Date().toISOString(),
        },
      };

      await ctx.call("dama_dispatcher.dispatch", event);
    },

    async submitViewMeta(ctx: Context) {
      const { params } = ctx;

      // @ts-ignore
      const { etl_context_id, user_id } = params;

      if (!(etl_context_id && user_id)) {
        throw new Error(
          "The etl_context_id and user_id parameters are required."
        );
      }

      const event = {
        type: EventTypes.VIEW_METADATA_SUBMITTED,
        payload: params,
        meta: {
          etl_context_id,
          user_id,
          timestamp: new Date().toISOString(),
        },
      };

      const result = await ctx.call("dama_dispatcher.dispatch", event);

      return result;
    },

    publishGisDatasetLayer,
  },
};
