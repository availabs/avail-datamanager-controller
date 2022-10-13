// FIXME: Need subEtlContexts for each layer

import { existsSync } from "fs";
import { readdir as readdirAsync } from "fs/promises";
import { join } from "path";

import { Context } from "moleculer";
import _ from "lodash";

import { FSA } from "flux-standard-action";

import etlDir from "../../constants/etlDir";

import GeospatialDatasetIntegrator from "../../../tasks/gis-data-integration/src/data_integrators/GeospatialDatasetIntegrator";

import serviceName from "./constants/serviceName";
import EventTypes from "./constants/EventTypes";
import ReadyToPublishPrerequisites from "./constants/ReadyToPublishPrerequisites";

import uploadGeospatialDataset from "./actions/uploadGeospatialDataset";
import stageLayerData from "./actions/stageLayerData";
import publishGisDatasetLayer from "./actions/publishGisDatasetLayer";

export default {
  name: serviceName,

  events: {
    [EventTypes.QA_APPROVED]: {
      context: true,
      // params: FSAEventParam,
      async handler(ctx: Context) {
        await this.checkIfReadyToPublish(ctx);
      },
    },

    [EventTypes.VIEW_METADATA_SUBMITTED]: {
      context: true,
      // params: FSAEventParam,
      async handler(ctx: Context) {
        await this.checkIfReadyToPublish(ctx);
      },
    },
  },

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

      await ctx.call("dama_dispatcher.dispatch", event);
    },

    publishGisDatasetLayer,
  },

  methods: {
    checkIfReadyToPublish: {
      async handler(ctx: Context) {
        const { params: event } = ctx;

        const {
          // @ts-ignore
          event_id,
          // @ts-ignore
          meta: { etl_context_id },
        } = event;

        // @ts-ignore
        const events: FSA[] = (
          await ctx.call("dama_dispatcher.queryDamaEvents", {
            etl_context_id,
          })
        ).filter(({ event_id: eid }) => eid <= event_id); // Filter out the future events

        const eventTypes = new Set(events.map(({ type }) => type));

        // console.log(JSON.stringify({ eventTypes: [...eventTypes] }, null, 4));

        if (ReadyToPublishPrerequisites.every((eT) => eventTypes.has(eT))) {
          const newEvent = {
            type: EventTypes.READY_TO_PUBLISH,
            payload: { etl_context_id },
            meta: (<FSA>event).meta,
          };

          process.nextTick(() =>
            ctx.call("dama_dispatcher.dispatch", newEvent)
          );

          return true;
        }

        return false;
      },
    },
  },
};
