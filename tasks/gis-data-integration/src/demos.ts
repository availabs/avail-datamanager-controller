import { createReadStream } from "fs";
import { join, basename } from "path";

import GeospatialDatasetIntegrator from "./data_integrators/GeospatialDatasetIntegrator";
import * as GeospatialDataUtils from "./utils/GeospatialDataUtils";
import * as OGRwkbGeometryType from "./utils/GeospatialDataUtils/OGRwkbGeometryType";

const samples = {
  highwaysShp: {
    archivedDatasetPath: join(
      __dirname,
      "../data/FreightAtlas/v20161108/highways.zip"
    ),
    layerName: "Highways",
    id: null,
  },
  fileGdbZip: {
    archivedDatasetPath: join(
      __dirname,
      "../data/FreightAtlas/v20161108/freight_atlas.zip"
    ),
    layerName: "Highways",
    id: "mushin:4918fe65-962c-41f4-bc1f-33b5130d750f",
  },

  alexLinesGeojson: {
    archivedDatasetPath: join(__dirname, "../lines.zip"),
    layerName: "lines",
    id: "mushin_c46f640f-9681-47dd-ac07-203f474e157b",
  },

  fileGdbTgz: {
    archivedDatasetPath: join(
      __dirname,
      "../data/FreightAtlas/v20161108/Map_Data.gdb.tgz"
    ),
    layerName: "MPO_Boundary",
    id: "mushin_4e59c468-911b-470d-b392-fbd82b76f406",
  },

  gpkgZip: {
    archivedDatasetPath: join(
      __dirname,
      "../data/FreightAtlas/v20161108/freight_atlas.gpkg.zip"
    ),
    layerName: "Major_Ports",
    id: null,
  },

  osmpTrailsGeoJSON: {
    archivedDatasetPath: join(__dirname, "../data/osmp_trails.geojson.zip"),
    layerName: "OSMP_Trails",
    id: null,
  },
};

const { archivedDatasetPath, layerName, id } = samples.alexLinesGeojson;

async function receive() {
  try {
    const gsdi = new GeospatialDatasetIntegrator();
    const rs = createReadStream(archivedDatasetPath);
    const fname = basename(archivedDatasetPath);

    await new Promise((resolve) => rs.once("open", resolve));

    const id = await gsdi.receiveDataset(fname, rs);
    console.log("id:", id);
  } catch (err) {
    console.error(err);
  }
}

async function analyze() {
  try {
    const gsdi = new GeospatialDatasetIntegrator(id);

    const metadata = await gsdi.getGeoDatasetMetadata();
    const analysis = await gsdi.getGeoDatasetLayerAnalysis(layerName);

    const defaultTableDescriptor =
      await gsdi.generateGeoDatasetLayerDefaultDatabaseTableDescriptor(
        layerName
      );

    // console.log(
    // JSON.stringify({ metadata, analysis, defaultTableDescriptor }, null, 4)
    // );
  } catch (err) {
    console.error(err);
  }
}

async function createTable() {
  try {
    const gsdi = new GeospatialDatasetIntegrator(id);

    // @ts-ignore
    await gsdi.createGeoDatasetLayerTable(layerName);
  } catch (err) {
    console.error(err);
  }
}

async function loadTable() {
  const gsdi = new GeospatialDatasetIntegrator(id);

  await gsdi.generateGeoDatasetLayerDefaultDatabaseTableDescriptor(
    layerName,
    true
  );

  await gsdi.loadTable(layerName, "development");
}

async function dumpGeoJSON() {
  const gsdi = new GeospatialDatasetIntegrator(id);

  await gsdi.dumpGeoDatasetLayerGeometriesGeoJSON(layerName, "development");
}

async function createGeoDatasetLayerMBTiles() {
  const gsdi = new GeospatialDatasetIntegrator(id);

  await gsdi.createGeoDatasetLayerMBTiles(layerName);
}

async function iterate() {
  const iter = GeospatialDataUtils.makeLayerFeaturesAsyncIterator(
    archivedDatasetPath,
    layerName
  );

  for await (const feature of iter) {
    const geom = feature.getGeometry();

    // console.log("name", geom.name);
    // console.log("coordinateDimension", geom.coordinateDimension);
    // console.log("dimension", geom.dimension);
    // // console.log("srs", geom.srs);
    // // console.log("wkbSize", geom.wkbSize);
    // console.log("wkbType", geom.wkbType);
    console.log(OGRwkbGeometryType.OGRGeometryTypeToName(geom.wkbType));
  }
}

async function receiveAndLoad() {
  try {
    const gsdi = new GeospatialDatasetIntegrator();
    const rs = createReadStream(archivedDatasetPath);
    const fname = basename(archivedDatasetPath);

    await new Promise((resolve) => rs.once("open", resolve));

    const _id = await gsdi.receiveDataset(fname, rs);
    console.log("id:", _id);
    await gsdi.loadTable(layerName);
  } catch (err) {
    console.error(err);
  }
}

// receive();
// analyze();
// loadTable();
// receiveAndLoad();
// dumpGeoJSON();
createGeoDatasetLayerMBTiles();
