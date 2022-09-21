import { createReadStream } from "fs";
import { join, basename } from "path";

import fetch from "node-fetch";
import FormData from "form-data";

import { TableDescriptor } from "../src/utils/GeospatialDataUtils";

const PORT = 5566;

const urlBase = `http://localhost:${PORT}/staged-geospatial-dataset`;

// Below are some examples. Modify as required.
// The following Geospatial Dataset formats are supported:
const samples = {
  highwaysShp: {
    archivedDatasetPath: join(
      __dirname,
      "../data/FreightAtlas/v20161108/highways.zip"
    ),
  },
  fileGdbZip: {
    archivedDatasetPath: join(
      __dirname,
      "../data/FreightAtlas/v20161108/freight_atlas.zip"
    ),
    // // These are for rerunning a given dataset
    layerName: "TruckParking",
    id: "mushin_02398632-e373-4f28-9237-4e15aa23d530",
  },

  fileGdbTgz: {
    archivedDatasetPath: join(
      __dirname,
      "../data/FreightAtlas/v20161108/Map_Data.gdb.tgz"
    ),
  },

  // GPKG not supported unless we switch from node-gdal to node-gdal-next or node-gdal-async
  // gpkgZip: {
  // archivedDatasetPath: join(
  // __dirname,
  // "../data/FreightAtlas/v20161108/freight_atlas.gpkg.zip"
  // ),
  // },

  osmpTrailsGeoJSON: {
    archivedDatasetPath: join(__dirname, "../data/osmp_trails.geojson.zip"),
  },
};

// const { archivedDatasetPath, layerName, id } = samples.fileGdbZip;
const { archivedDatasetPath, layerName, id } = samples.fileGdbZip;

async function integrateNewGeospatialDataset() {
  try {
    const formData = new FormData();
    formData.append("file", createReadStream(archivedDatasetPath), {
      filename: basename(archivedDatasetPath),
    });

    // Upload the Geospatial Dataset
    const idRes = await fetch(`${urlBase}/uploadGeospatialDataset`, {
      method: "POST",
      body: formData,
    });

    // Upload response is the ETL ID
    const { id }: { id: string } = await idRes.json();

    // Get the Geospatial Dataset layer names
    const layerNamesRes = await fetch(`${urlBase}/${id}/layerNames`);

    const layerNames: string[] = await layerNamesRes.json();

    console.log(JSON.stringify(layerNames, null, 4));

    // Select random layer
    const i = Math.floor(Math.random() * layerNames.length);

    const layerName = layerNames[i];

    console.log("layerName:", layerName);
    const tableDescriptorRes = await fetch(
      `${urlBase}/${id}/${layerName}/tableDescriptor`
    );

    // The tableDescriptor controls DB table creation and loading.
    const tableDescriptor: TableDescriptor = await tableDescriptorRes.json();

    const nonAlphaNumRE = /[^0-9a-z]/gi;

    // We modify the tableDescriptor to simulate user editing via UI.
    tableDescriptor.tableSchema = "staged_geospatial_datasets";
    tableDescriptor.tableName = tableDescriptor.tableName.replace(
      nonAlphaNumRE,
      "__"
    );

    tableDescriptor.columnTypes = tableDescriptor.columnTypes.map((d) => ({
      ...d,
      col: d.col.replace(nonAlphaNumRE, "__"),
    }));

    // Replace the default tableDescriptor with the modified one.
    await fetch(`${urlBase}/${id}/updateTableDescriptor`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(tableDescriptor),
    });

    // Load the database table using the modified tableDescriptor
    await fetch(`${urlBase}/${id}/${layerName}/loadDatabaseTable`);

    // Will download tippecanoe if not already in ../lib/tippecanoe/
    await fetch(`${urlBase}/${id}/${layerName}/createMBTiles`);

    // Add API routes to dataset-integration-server and MBTiles to staged-geospatial-dataset-tileserver
    await stageGeospatialDatasetLayer(id, layerName);
  } catch (err) {
    console.error(err);
  }
}

async function stageGeospatialDatasetLayer(_id = id, _layerName = layerName) {
  try {
    const routeResp = await fetch(`${urlBase}/${_id}/${_layerName}/stage`);

    const resp = await routeResp.json();

    console.log(JSON.stringify(resp, null, 4));

    const { getByIdRoute } = resp;

    const r = `http://localhost:${PORT}${getByIdRoute.replace(
      /:featureId/,
      0
    )}`;

    const getResp = await fetch(r);
    console.log(JSON.stringify(await getResp.json(), null, 4));

    // const dumpResp = await fetch(`http://localhost:${PORT}${dumpGeoJSONDRoute}`);
    // dumpResp.body.pipe(process.stdout);
  } catch (err) {
    console.error(err);
  }
}

integrateNewGeospatialDataset();
// stageGeospatialDatasetLayer();
