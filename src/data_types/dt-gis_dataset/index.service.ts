import { Context } from "moleculer";
import uploadFile from './upload/upload'
import publish from './publish/publish'

import GeospatialDatasetIntegrator from "../../../tasks/gis-data-integration/src/data_integrators/GeospatialDatasetIntegrator";

export const serviceName = 'gis-dataset'


export default {
  name: serviceName,
  actions: {
    
    //uploads a file and unzips it into tmp-etl
    uploadFile,
  
    //these routes get data from fs based on id passed to client in upload route 
    getLayerNames(ctx) {
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

    async getTableDescriptor(ctx) {
      const {
        // @ts-ignore
        params: { id, layerName },
      } = ctx;

      const gdi = new GeospatialDatasetIntegrator(id);

      const tableDescriptor = await gdi.getLayerTableDescriptor(layerName);

      return tableDescriptor;
    },

    async getLayerAnalysis(ctx) {
      const {
        // @ts-ignore
        params: { id, layerName },
      } = ctx;

      const gdi = new GeospatialDatasetIntegrator(id);

      const layerAnalysis = await gdi.getGeoDatasetLayerAnalysis(layerName);

      return layerAnalysis;
    },

    publish

  }
}