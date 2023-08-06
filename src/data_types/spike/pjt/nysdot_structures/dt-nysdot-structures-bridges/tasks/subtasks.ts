import { createReadStream } from "fs";
import { basename } from "path";

import { getPgEnv } from "data_manager/contexts";

import GeospatialDatasetIntegrator from "../../../../../../../tasks/gis-data-integration/src/data_integrators/GeospatialDatasetIntegrator";

import { TableDescriptor } from "../../../../../../../tasks/gis-data-integration/src/utils/GeospatialDataUtils";

export async function extract(file_path: string) {
  const file_stream = createReadStream(file_path);
  const file_name = basename(file_path);

  const gdi = new GeospatialDatasetIntegrator();

  return gdi.receiveDataset(<string>file_name, file_stream);
}

export async function getGeoDatasetMetadata(id: string) {
  const gdi = new GeospatialDatasetIntegrator(id);

  const metadata = gdi.getGeoDatasetMetadata();

  return metadata;
}

export async function getLayerNames(id: string) {
  const gdi = new GeospatialDatasetIntegrator(id);

  // @ts-ignore
  const { layerNameToId } = gdi;
  const layer_names = Object.keys(layerNameToId);

  return layer_names;
}

export async function updateLayerNamesToIds(
  id: string,
  layer_name_to_id: Record<string, number>
) {
  const gdi = new GeospatialDatasetIntegrator(id);

  // @ts-ignore
  gdi.layerNameToId = layer_name_to_id;
}

export async function persistLayerTableDescriptor(
  id: string,
  table_descriptor: TableDescriptor
) {
  const gdi = new GeospatialDatasetIntegrator(id);

  // @ts-ignore
  await gdi.persistLayerTableDescriptor(table_descriptor);
}

export async function getTableDescriptor(id: string, layer_name: string) {
  const gdi = new GeospatialDatasetIntegrator(id);

  const tableDescriptor = await gdi.getLayerTableDescriptor(layer_name);

  return tableDescriptor;
}

export async function loadLayer(id: string, layer_name: string) {
  const gdi = new GeospatialDatasetIntegrator(id);

  const { tableSchema: table_schema, tableName: table_name } =
    await gdi.loadTable({
      layerName: layer_name,
      pgEnv: getPgEnv(),
    });

  return { table_schema, table_name };
}
