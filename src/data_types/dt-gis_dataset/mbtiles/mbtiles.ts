import { difference, uniq } from "lodash";
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

import dama_db from "data_manager/dama_db";
import dama_events from "data_manager/events";
import dama_meta from "data_manager/meta";
import logger from "data_manager/logger";

import { getPgEnv } from "data_manager/contexts";
import { NodePgQueryResult } from "data_manager/dama_db/postgres/PostgreSQL";
import installTippecanoe from "../../../data_utils/gis/tippecanoe/bin/installTippecanoe";
import { etlDir, mbtilesDir, libDir } from "../../../constants";
import { getStyleFromJsonType } from "./default-styles";

const tippecanoePath = join(libDir, "tippecanoe/tippecanoe");

function asyncGeneratorToNdjsonStream(iter) {
  async function* toNdjson() {
    for await (const feature of iter) {
      yield `${JSON.stringify(feature)}\n`;
    }
  }

  return Readable.from(toNdjson(), { objectMode: false });
}

const pipelineAsync = promisify(pipeline);

export async function createViewMbtiles(
  damaViewId: number,
  damaSourceId: number,
  etlContextId: number
) {
  const { path: etlWorkDir, cleanupCallback: eltWorkDirCleanup }: any =
    await new Promise((resolve, reject) =>
      tmp.dir({ tmpdir: etlDir }, (err, path, cleanupCallback) => {
        if (err) {
          return reject(err);
        }
        resolve({ path, cleanupCallback });
      })
    );

  const pg_env = getPgEnv();
  logger.info(`pg Env: ${pg_env}`);

  const layerName = `s${damaSourceId}_v${damaViewId}`;
  const timestamp = new Date().getTime();

  const tilesetName = `${pg_env}_${layerName}_${timestamp}`;
  const mbtilesFileName = `${tilesetName}.mbtiles`;
  const mbtilesFilePath = join(etlWorkDir, mbtilesFileName);

  const initialEvent = {
    type: "gis-dataset:CREATE_MBTILES_INITIAL",
    payload: {
      damaViewId,
      timestamp,
    },
    meta: {
      etl_context_id: etlContextId,
      timestamp: new Date(),
    },
  };

  await dama_events.dispatch(initialEvent, etlContextId);

  const featuresAsyncIterator =
    makeDamaGisDatasetViewGeoJsonFeatureAsyncIterator(damaViewId, {
      properties: ["ogc_fid"],
    });

  logger.debug(
    "\n\nfeaturesAsyncIterator inside createViewMbtiles():",
    featuresAsyncIterator
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
    const source_layer_name = layerName;

    const geojson_type = await dama_meta.getDamaViewProperties(damaViewId, [
      "geojson_type",
    ]);

    logger.debug("\n\ngeojson_type inside createViewMbtiles():", geojson_type);
    const tiles = {
      tiles: {
        sources: [
          {
            id: tilesetName,
            source: {
              url: `$HOST/data/${tilesetName}.json`,
              type: "vector",
            },
          },
        ],
        layers: [
          {
            id: source_layer_name,
            ...getStyleFromJsonType(geojson_type),
            source: tilesetName,
            "source-layer": source_layer_name,
          },
        ],
      },
    };

    await dama_db.query({
      text: `UPDATE data_manager.views SET metadata = COALESCE(metadata,'{}') || '${JSON.stringify(
        tiles
      )}'::jsonb WHERE view_id = $1;`,
      values: [damaViewId],
    });

    const finalEvent = {
      type: "dataset:CREATE_MBTILES_FINAL",
      payload: {
        view_id: damaViewId,
        tippecanoeStdout,
        tippecanoeStderr,
      },
      meta: {
        etl_context_id: etlContextId,
        timestamp: new Date(),
      },
    };

    logger.debug("\n\nshould it dispatch final event?:", finalEvent);
    await dama_events.dispatch(finalEvent, etlContextId);
  } catch (err) {
    const { message, stack } = <Error>err;
    const errorEvent = {
      type: "createDamaGisDatasetViewMbtiles:ERROR",
      payload: { message, stack },
      meta: {
        etl_context_id: etlContextId,
        timestamp: new Date(),
      },
      error: true,
    };

    await dama_events.dispatch(errorEvent, etlContextId);

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
  }: any = await new Promise((resolve, reject) =>
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

export async function getDamaGisDatasetViewTableSchemaSummary(
  damaViewId: number
) {
  const damaViewPropsColsQ = dedent(`
    SELECT
        column_name,
        is_geometry_col
      FROM _data_manager_admin.dama_table_column_types
      WHERE (
        ( view_id = $1 )
      )
  `);

  console.log("getDamaGisDatasetViewTableSchemaSummary", damaViewId);
  const { rows } = await dama_db.query({
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

  const damaViewIntIdRes: NodePgQueryResult = await dama_db.query({
    text: damaViewIntIdQ,
    values: [damaViewId],
  });

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
}

export async function generateGisDatasetViewGeoJsonSqlQuery(
  damaViewId: number,
  config: any = {}
) {
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
  } = await getDamaGisDatasetViewTableSchemaSummary(damaViewId);

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
    const invalidProps = difference(properties, nonGeometryColumns);

    if (invalidProps.length) {
      throw new Error(
        `The following requested properties are not in the DamaView's data table: ${invalidProps}`
      );
    }

    props = uniq(properties);
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

  const selectColsClause = uniq([intIdColName, geometryColumn, ...props])
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

  console.log("feature generator sql", sql);

  return sql;
}

export async function* makeDamaGisDatasetViewGeoJsonFeatureAsyncIterator(
  damaViewId: number,
  config = {}
) {
  const sql = await generateGisDatasetViewGeoJsonSqlQuery(damaViewId, config);

  const iter = <AsyncGenerator<any>>dama_db.makeIterator(sql, config);

  for await (const { feature } of iter) {
    yield feature;
  }
}
