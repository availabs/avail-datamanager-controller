import dedent from "dedent";
import pgFormat from "pg-format";
import { spawn } from "child_process";
import { existsSync, createWriteStream } from "fs";
import { pipeline } from "stream";
import { promisify } from "util";
import { createGzip } from "zlib";
import { Readable } from "stream";
import { rename as renameAsync } from "fs/promises";
import { join, basename } from "path";
import tmp from "tmp";

import installTippecanoe from "../../../data_utils/gis/tippecanoe/bin/installTippecanoe";

import { etlDir, mbtilesDir, libDir } from "../../../constants";

// path to tippecanoe executable
const tippecanoePath = join(libDir, "tippecanoe/tippecanoe")

function asyncGeneratorToNdjsonStream(iter) {
  async function* toNdjson() {
    for await (const feature of iter) {
      yield `${JSON.stringify(feature)}\n`;
    }
  }

  return Readable.from(toNdjson(), { objectMode: false });
}

const pipelineAsync = promisify(pipeline);

// export type CreateMBTilesConfig = {
//   layerName: string;
//   mbtilesFilePath: string;
//   featuresAsyncIterator: AsyncGenerator<Feature>;
//   etlWorkDir?: string;
// };

export async function createGisDatasetViewMbtiles(ctx, etl_context_id, damaViewId, damaSourceId) {
  const {
    // @ts-ignore
    meta: { 
      pgEnv
    }
  } = ctx;

  console.log('createGisDatasetViewMbtiles context_id: ', etl_context_id, ctx.meta)

  const { path: etlWorkDir, cleanupCallback: eltWorkDirCleanup } =
    await new Promise((resolve, reject) =>
      tmp.dir({ tmpdir: etlDir }, (err, path, cleanupCallback) => {
        if (err) {
          return reject(err);
        }
        resolve({ path, cleanupCallback });
      })
    );

  const layerName = `s${damaSourceId}_v${damaViewId}`;
  const timestamp = new Date().getTime()

  const tilesetName = `${pgEnv}_${layerName}_${timestamp}`;
  const mbtilesFileName = `${tilesetName}.mbtiles`;
  const mbtilesFilePath = join(etlWorkDir, mbtilesFileName);

  const initialEvent = {
    type: "gis-dataset:CREATE_MBTILES_INITIAL",
    payload: {
      damaViewId,
      timestamp,
    },
    meta: { etl_context_id },
  };

  await ctx.call("data_manager/events.dispatch", initialEvent);

  const featuresAsyncIterator = (
    await ctx.call(
      "gis-dataset.makeDamaGisDatasetViewGeoJsonFeatureAsyncIterator",
      ctx.params
    )
  );

  try {
    const { tippecanoeArgs, tippecanoeStdout, tippecanoeStderr } =
      await createMbtilesTask({
        layerName,
        mbtilesFilePath,
        featuresAsyncIterator,
        etlWorkDir,
      });

    const mbtilesBaseName = basename(mbtilesFilePath);
    const servedMbtilesPath = join(mbtilesDir, mbtilesBaseName);

    await renameAsync(mbtilesFilePath, servedMbtilesPath);

    const source_id = damaViewGlobalId;
    const source_layer_name = layerName;
    const source_type = "vector";

    const geojson_type = await ctx.call("dama/metadata.getDamaViewProperties", {
      damaViewId, properties: ["geojson_type"]
    })

    const newRow = {
      view_id: damaViewId,
      geojson_type,
      tileset_name: tilesetName,
      source_id,
      source_layer_name,
      source_type,
      tippecanoe_args: JSON.stringify(tippecanoeArgs),

    };

    console.log('mbtiles created', newRow)

    // const {
    //   rows: [{ mbtiles_id }],
    // } = await ctx.call("dama_db.insertNewRow", {
    //   tableSchema: "_data_manager_admin",
    //   tableName: "dama_views_mbtiles_metadata",
    //   newRow,
    // });

    const finalEvent = {
      type: "dataset:CREATE_MBTILES_FINAL",
      payload: {
        view_id: damaViewId,
        mbtiles_id,
        tippecanoeStdout,
        tippecanoeStderr,
      },
      meta: { etl_context_id, timestamp: new Date() },
    };

    await ctx.call("data_manager/events.dispatch", finalEvent);
  } catch (err) {
    const errorEvent = {
      type: "createDamaGisDatasetViewMbtiles:ERROR",
      payload: { message: err.message },
      error: true,
      meta: { etl_context_id },
    };

    await ctx.call("data_manager/events.dispatch", errorEvent);

    throw err;
  } finally {
    await eltWorkDirCleanup();
  }
}


export async function createMbtilesTask({
  layerName,
  mbtilesFilePath,
  featuresAsyncIterator,
  etlWorkDir,
}) {
  if (!etlWorkDir) {
    etlWorkDir = await new Promise((resolve, reject) =>
      tmp.dir({ tmpdir: etlDir }, (err, path) => {
        if (err) {
          return reject(err);
        }

        resolve(path);
      })
    );
  }

  const {
    path: geojsonFilePath,
    fd: geojsonFileDescriptor,
    cleanupCallback: geojsonFileCleanup,
  } = await new Promise((resolve, reject) =>
    tmp.file(
      { tmpdir: etlWorkDir, postfix: ".geojson.gz" },
      (err, path, fd, cleanupCallback) => {
        if (err) {
          return reject(err);
        }

        resolve({ path, fd, cleanupCallback });
      }
    )
  );

  // @ts-ignore
  const ws = createWriteStream(null, {
    fd: geojsonFileDescriptor,
  });

  const gzip = createGzip({ level: 9 });
  await pipelineAsync(
    asyncGeneratorToNdjsonStream(featuresAsyncIterator),
    gzip,
    ws
  );

  let success: Function;
  let fail: Function;

  if (!existsSync(tippecanoePath)) {
    console.log("Installing tippecanoe...");
    await installTippecanoe();
  }

  const done = new Promise((resolve, reject) => {
    success = resolve;
    fail = reject;
  });

  const name = basename(mbtilesFilePath, ".mbtiles");

  const tippecanoeArgs = [
    "--no-progress-indicator",
    "--read-parallel",
    "--no-feature-limit",
    "--no-tile-size-limit",
    "--generate-ids",
    "-r1",
    "--force",
    "--name",
    name,
    "--layer",
    layerName,
    "-o",
    mbtilesFilePath,
    geojsonFilePath,
  ];

  const tippecanoeCProc = spawn(tippecanoePath, tippecanoeArgs, {
    stdio: "pipe",
  })
    // @ts-ignore
    .once("error", fail)
    // @ts-ignore
    .once("close", success);

  let tippecanoeStdout = "";

  /* eslint-disable-next-line no-unused-expressions */
  tippecanoeCProc.stdout?.on("data", (data) => {
    // process.stdout.write(data);
    tippecanoeStdout += data.toString();
  });

  let tippecanoeStderr = "";

  /* eslint-disable-next-line no-unused-expressions */
  tippecanoeCProc.stderr?.on("data", (data) => {
    // process.stderr.write(data);
    tippecanoeStderr += data.toString();
  });

  await done;

  await geojsonFileCleanup();

  return {
    layerName,
    mbtilesFilePath,
    geojsonFilePath,
    tippecanoeArgs,
    tippecanoeStdout,
    tippecanoeStderr,
  };
}

export async function createView(ctx, view_values) {
 
  const {
    source_id,
    user_id
  } = view_values

  let newDamaView = (
    await ctx.call("dama/metadata.createNewDamaView", {source_id, user_id})
  );

  const {
    view_id: damaViewId,
    table_schema: origTableSchema,
    table_name: origTableName,
  } = newDamaView;

  const table_schema = origTableSchema || "gis_datasets";
  let table_name = origTableName;

  // Assign the default table_name if one wasn't specified
  if (!origTableName) {
    const text =
      "SELECT _data_manager_admin.dama_view_name($1) AS dama_view_name;";

    const {
      rows: [{ dama_view_name }],
    } = await ctx.call("dama_db.query", {
      text,
      values: [damaViewId],
    });

    table_name = dama_view_name;
  }

  if (origTableSchema !== table_schema || origTableName !== table_name) {
    const updateViewMetaSql = dedent(
      `
          UPDATE data_manager.views
            SET
              table_schema  = $1,
              table_name    = $2,
              data_table    = $3
            WHERE ( view_id = $4 )
        `
    );

    const dataTable = pgFormat("%I.%I", table_schema, table_name);

    const q = {
      text: updateViewMetaSql,
      values: [table_schema, table_name, dataTable, damaViewId],
    };

    await ctx.call("dama_db.query", q);

    const { rows } = await ctx.call("dama_db.query", {
      text: "SELECT * FROM data_manager.views WHERE ( view_id = $1 );",
      values: [damaViewId],
    });

    newDamaView = rows[0];
  }

  return newDamaView;
}

export async function createSource(ctx, source_values) {
  // const uniqId = uuid().replace(/[^0-9A-Z]/gi, "");
  const {
      name, // = `untitled dataset ${uniqId}`,
      type = 'gis_dataset',
      update_interval =  '',
      description = '',
      statistics = {},
      metadata = {},
  } = source_values
  // create source
  let damaSource = await ctx.call(
      "dama/metadata.createNewDamaSource",
      {
        name,
        type,
        update_interval,
        description,
        statistics,
        metadata
      }
  )
  return damaSource
}
