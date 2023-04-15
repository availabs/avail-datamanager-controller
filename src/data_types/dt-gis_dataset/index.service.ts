import { Context } from "moleculer";
import uploadFile from './upload/upload'
import publish from './publish/publish'
import {
  createViewMbtiles,
  getDamaGisDatasetViewTableSchemaSummary,
  generateGisDatasetViewGeoJsonSqlQuery,
  makeDamaGisDatasetViewGeoJsonFeatureAsyncIterator
} from  './mbtiles/mbtiles'

import {
  getLayerNames,
  getTableDescriptor,
  getLayerAnalysis
} from './stage/stageLayer'


import GeospatialDatasetIntegrator from "../../../tasks/gis-data-integration/src/data_integrators/GeospatialDatasetIntegrator";

export const serviceName = 'gis-dataset'


export default {
  name: serviceName,
  actions: {
    //--------------------------------------------
    // -- UPLOAD function
    // uploads a file and unzips it into tmp-etl
    // then returns the client a staged_data_id
    //---------------------------------------------
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

    //-----------------------------------------------
    // -- PUBLISH function
    // takes staged_data_id, and params
    // updates datamanager and writes data to db
    // (not currently atomic sorry Paul)
    // by default automatically calls crateViewMbTiles
    //-----------------------------------------------
    publish,

    // -----------------------------------------------
    // -- MBTILES functions
    // Creates Mbtiles given source / view Id
    // by streaming data from db to tippacanoe
    // writes metadata to views.metadata.tiles
    // -----------------------------------------------
    createViewMbtiles,
    getDamaGisDatasetViewTableSchemaSummary,
    generateGisDatasetViewGeoJsonSqlQuery,
    makeDamaGisDatasetViewGeoJsonFeatureAsyncIterator

  }
}
