import _, { rest } from 'lodash'
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

import { getStyleFromJsonType } from './default-styles'

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

export async function createViewMbtiles(ctx) {
  const {
    // @ts-ignore
    params: {
      damaViewId,
      damaSourceId
    },
    meta: {
      pgEnv,
      etl_context_id
    }
  } = ctx;

  console.log('createGisDatasetViewMbtiles context_id: ',
    etl_context_id,
    ctx.meta,
    damaViewId,
    damaSourceId)

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
      { ...ctx.params, config: { properties: ['ogc_fid'] } }
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

    //const source_id = damaViewGlobalId;
    const source_layer_name = layerName;
    const source_type = "vector";

    const geojson_type = await ctx.call("dama/metadata.getDamaViewProperties", {
      damaViewId, properties: ["geojson_type"]
    })

    //console.log


    // const newRow = {
    //   view_id: damaViewId,
    //   geojson_type,
    //   tileset_name: tilesetName,
    //   source_layer_name,
    //   source_type,
    //   tippecanoe_args: JSON.stringify(tippecanoeArgs),

    // };

    //console.log('mbtiles created', newRow)

    const tiles = {
      "tiles": {
        "sources": [
          {
            "id": tilesetName,
            "source": {
               "url": `$HOST/data/${tilesetName}.json`,
               "type": "vector"
            }
          }
        ],
        "layers": [
           {
              "id": source_layer_name,
              ...getStyleFromJsonType(geojson_type),
              "source": tilesetName,
              "source-layer": source_layer_name
           }
        ]
      }
    }

    // console.log('tiles', tiles, damaViewId)
    let results = await ctx.call("dama_db.query", {
      text: `UPDATE data_manager.views SET metadata = COALESCE(metadata,'{}') || '${JSON.stringify(tiles)}'::jsonb WHERE view_id = $1;`,
      values: [damaViewId],
    });

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
        tippecanoeStdout,
        tippecanoeStderr,
      },
      meta: { etl_context_id, timestamp: new Date() },
    };

    console.log('final event stff')

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

export const getDamaGisDatasetViewTableSchemaSummary  = {
      async handler(ctx) {
        const {
          // @ts-ignore
          params: { damaViewId },
        } = ctx;

        const damaViewPropsColsQ = dedent(`
          SELECT
              column_name,
              is_geometry_col
            FROM _data_manager_admin.dama_table_column_types
            WHERE (
              ( view_id = $1 )
            )
        `);
        console.log('getDamaGisDatasetViewTableSchemaSummary', damaViewId)
        const { rows } = await ctx.call("dama_db.query", {
          text: damaViewPropsColsQ,
          values: [damaViewId],
        });

        if (rows.length === 0) {
          throw new Error(`Invalid DamaViewId: ${damaViewId}`);
        }

        const nonGeometryColumns = rows
          .filter(({ is_geometry_col }) => !is_geometry_col)
          .map(({ column_name }) => column_name);

        const { column_name: geometryColumn = null } =
          rows.find(({ is_geometry_col }) => is_geometry_col) || {};

        const damaViewIntIdQ = dedent(`
          SELECT
              table_schema,
              table_name,
              primary_key_summary,
              int_id_column_name
            FROM _data_manager_admin.dama_views_int_ids
            WHERE ( view_id = $1 )
        `);

        const damaViewIntIdRes: NodePgQueryResult = await ctx.call(
          "dama_db.query",
          {
            text: damaViewIntIdQ,
            values: [damaViewId],
          }
        );

        if (!damaViewIntIdRes.rows.length) {
          throw new Error(
            `Unable to get primary key metadata for DamaView ${damaViewId}`
          );
        }

        const {
          rows: [
            {
              table_schema: tableSchema,
              table_name: tableName,
              primary_key_summary,
              int_id_column_name: intIdColName,
            },
          ],
        } = damaViewIntIdRes;

        const primaryKeyCols = primary_key_summary.map(
          ({ column_name }) => column_name
        );

        return {
          tableSchema,
          tableName,
          primaryKeyCols,
          intIdColName,
          nonGeometryColumns,
          geometryColumn,
        };
      },
    },

export const generateGisDatasetViewGeoJsonSqlQuery = async function generateGisDatasetViewGeoJsonSqlQuery(ctx) {
  const {
    // @ts-ignore
    params: { damaViewId, config = {} },
  } = ctx;

  console.log('generateGisDatasetViewGeoJsonSqlQuery', config, ctx.params)

  let { properties } = config;

  if (properties && properties !== "*" && !Array.isArray(properties)) {
    properties = [properties];
  }

  const {
    tableSchema,
    tableName,
    intIdColName,
    nonGeometryColumns,
    geometryColumn,
  } = await ctx.call('gis-dataset.getDamaGisDatasetViewTableSchemaSummary',
    {
      damaViewId ,
      parentCtx: ctx
    }
  );

  if (!geometryColumn) {
    throw new Error(
      `DamaView's ${damaViewId} does not appear to be a GIS dataset.`
    );
  }

  let props: string[] = [];

  if (properties === "*") {
    props = nonGeometryColumns;
  }

  if (Array.isArray(properties)) {
    const invalidProps = _.difference(properties, nonGeometryColumns);

    if (invalidProps.length) {
      throw new Error(
        `The following requested properties are not in the DamaView's data table: ${invalidProps}`
      );
    }

    props = _.uniq(properties);
  }

  const featureIdBuildObj = {
    text: intIdColName ? "'id', %I," : "",
    values: intIdColName ? [intIdColName] : [],
  };

  const propsBuildObj = props.reduce(
    (acc, prop) => {
      acc.placeholders.push("%L");
      acc.placeholders.push("%I");

      acc.values.push(prop, prop);

      return acc;
    },
    { placeholders: <string[]>[], values: <string[]>[] }
  );

  const selectColsClause = _.uniq([intIdColName, geometryColumn, ...props])
    .filter(Boolean)
    .reduce(
      (acc, col) => {
        acc.placeholders.push("%I");
        acc.values.push(col);

        return acc;
      },
      { placeholders: [], values: [] }
    );

  const sql = pgFormat(
    `
        SELECT
            jsonb_build_object(
              'type',       'Feature',
              ${featureIdBuildObj.text}
              'properties', jsonb_build_object(${propsBuildObj.placeholders}),
              'geometry',   ST_AsGeoJSON(%I)::JSON
            ) AS feature
          FROM (
            SELECT ${selectColsClause.placeholders}
              FROM %I.%I
          ) row;
      `,
    ...featureIdBuildObj.values,
    ...propsBuildObj.values,
    geometryColumn,
    ...selectColsClause.values,
    tableSchema,
    tableName
  );

  console.log('feature generator sql', sql)

  return sql;
}

export const makeDamaGisDatasetViewGeoJsonFeatureAsyncIterator = {
  async *handler(ctx) {
    const {
      // @ts-ignore
      params: { config = {} },
    } = ctx;

    console.log('makeDamaGisDatasetViewGeoJsonFeatureAsyncIterator', config, ctx.params)
    const sql = await ctx.call('gis-dataset.generateGisDatasetViewGeoJsonSqlQuery',
      ctx.params
    );

    const iter = <AsyncGenerator<any>>await ctx.call(
      "dama_db.makeIterator",
      {
        query: sql,
        config,
      }
    );

    for await (const { feature } of iter) {
      yield feature;
    }
  }
}
