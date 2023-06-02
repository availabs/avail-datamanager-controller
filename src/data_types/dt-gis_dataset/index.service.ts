import { Context } from "moleculer";
import uploadFile from './upload/upload'
import publish from './publish/publish'
import createDownload from './createDownload/createDownload'
import dama_host_id from "constants/damaHostId";
import dama_events from "data_manager/events";
import { join } from "path";

import logger from "data_manager/logger";
import { QueuedDamaTaskDescriptor } from "data_manager/tasks/domain";

import {
  createViewMbtiles,
  getDamaGisDatasetViewTableSchemaSummary,
  generateGisDatasetViewGeoJsonSqlQuery,
  makeDamaGisDatasetViewGeoJsonFeatureAsyncIterator,
} from "./mbtiles/mbtiles2";

import {
  getLayerNames,
  getTableDescriptor,
  getLayerAnalysis,
} from "./stage/stageLayer";

import uploadFile from "./upload/upload";
import { createSource } from "./publish/actions";


export const serviceName = "gis-dataset";

export default {
  name: serviceName,
  actions: {
    // --------------------------------------------
    // -- UPLOAD function
    // uploads a file and unzips it into tmp-etl
    // then returns the client a staged_data_id
    // ---------------------------------------------
    uploadFile,
    // --------------------------------------------
    // -- STAGING Routes
    // these routes recieve staged_data_id
    // to return data to client so user can
    // make choices about how to publish
    // --------------------------------------------
    getLayerNames,
    getTableDescriptor,
    getLayerAnalysis,

    // -----------------------------------------------
    // -- PUBLISH function
    // takes staged_data_id, and params
    // updates datamanager and writes data to db
    // (not currently atomic sorry Paul)
    // by default automatically calls crateViewMbTiles
    // -----------------------------------------------

    publish: {
      visibility: "published",

      async handler(ctx) {
        let damaSource: any;
        // @ts-ignore
        let source_id: number = ctx?.params?.source_id;
        const isNewSourceCreate: boolean = source_id ? false : true;

        logger.info(`\nFirst Call:  ${ctx?.params}`);
        // @ts-ignore
        if (!source_id) {
          // @ts-ignore
          damaSource = await createSource(ctx?.params?.source_values);
          logger.info(`\nNew Source Created in publish:  ${damaSource}`);
          source_id = damaSource?.source_id;
          // @ts-ignore
          ctx.params.source_id = source_id;
          logger.info("Attached source_id to ctx: ", damaSource);
        }

        const worker_path = join(__dirname, "./publish/publish.worker.ts");

        const dama_task_descr = {
          worker_path,
          // @ts-ignore
          parent_context_id: null,
          source_id,
          initial_event: {
            type: ":INITIAL",
            payload: Object.assign({}, ctx?.params, { isNewSourceCreate }),
            meta: {
              __dama_task_manager__: {
                dama_host_id,
                worker_path,
              },
            },
          },
        };

        const options = { retryLimit: 0, expireInSeconds: 30000 };

        // @ts-ignore
        const { etl_context_id } = await ctx.call("dama/tasks.queueDamaTask", {
          dama_task_descr,
          options,
        });

        return { etl_context_id, source_id };
      },
    },

    getTaskFinalEvent: {
      visibility: "published",

      async handler(ctx) {
        // @ts-ignore
        const etl_context_id = ctx?.params?.etlContextId;

        try {
          logger.info("\n\n\ncalled getTaskFinalEvent\n\n");
          const finalEvent = await dama_events.getEtlContextFinalEvent(
            etl_context_id
          );
          logger.info(`\nfinalEvent:: \n: ${finalEvent}`);
          return finalEvent;
        } catch (error) {
          return null;
        }
      },
    },

    // -----------------------------------------------
    // -- MBTILES functions
    // Creates Mbtiles given source / view Id
    // by streaming data from db to tippacanoe
    // writes metadata to views.metadata.tiles
    // -----------------------------------------------
    createViewMbtiles,
    getDamaGisDatasetViewTableSchemaSummary,
    generateGisDatasetViewGeoJsonSqlQuery,
    makeDamaGisDatasetViewGeoJsonFeatureAsyncIterator,
<<<<<<< HEAD

    //------------------------------------------------
    // -- export Downloads 
    // 
    //
    //------------------------------------------------
    createDownload
  }
}
=======
  },
};
>>>>>>> e408b1e6edb394df9e803de3f24ee4e7b740fa45
