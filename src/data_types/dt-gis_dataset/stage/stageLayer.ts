import GeospatialDatasetIntegrator from "../../../../tasks/gis-data-integration/src/data_integrators/GeospatialDatasetIntegrator";

export const getLayerNames = function getLayerNames(ctx) {
  const {
    // @ts-ignore
    params: { id },
  } = ctx;

  const gdi = new GeospatialDatasetIntegrator(id);

  // @ts-ignore
  const layerNameToId = gdi.layerNameToId;
  const layerNames = Object.keys(layerNameToId);

  return layerNames;
}

export const getTableDescriptor = async function getTableDescriptor(ctx) {
  const {
    // @ts-ignore
    params: { id, layerName },
  } = ctx;

  const gdi = new GeospatialDatasetIntegrator(id);

  const tableDescriptor = await gdi.getLayerTableDescriptor(layerName);

  return tableDescriptor;
}

export const getLayerAnalysis = async function getLayerAnalysis(ctx) {
  const {
    // @ts-ignore
    params: { id, layerName },
  } = ctx;

  const gdi = new GeospatialDatasetIntegrator(id);
  const layerAnalysis = await gdi.getGeoDatasetLayerAnalysis(layerName);

  return layerAnalysis;
}