import { spawn } from "child_process";
import { existsSync } from "fs";
import {
  readFile as readFileAsync,
  writeFile as writeFileAsync,
} from "fs/promises";

import gdal from "gdal-async";
import pgFormat from "pg-format";
import _ from "lodash";
import tmp from "tmp";
import dedent from "dedent";

import * as GeoJSON from "geojson";

import analyzeSchema, {
  PgDataType,
  pgIntegerTypes,
} from "../../../../../src/data_utils/analysis/analyzeSchema";

import { getPostgresConnectionString } from "../../../../../src/data_manager/dama_db/postgres/PostgreSQL";

import {
  OGRwkbGeometryType,
  OGRGeometryTypeToName,
  OGR_GT_GetCollection,
  OGRMergeGeometryTypes,
  wkbHasZ,
  wkbHasM,
  simplifyOGRwkbGeometryType,
} from "./OGRwkbGeometryType";

import {
  PostGisGeometryTypes,
  PostGisDimension,
  OGRwkbGeometryTypeToPostGisType,
  getPostGisDimensionForOGRwkbGeometryType,
} from "./PostGisGeometryType";

gdal.verbose();

const PostGisRootGeometryTypes = new Set([
  PostGisGeometryTypes.Geometry,
  PostGisGeometryTypes.GeometryM,
  PostGisGeometryTypes.GeometryZ,
  PostGisGeometryTypes.GeometryZM,
]);

export const GEODATASET_METADATA_VERSION = "0.0.1";
export const GEODATASET_ANALYSIS_VERSION = "0.0.1";

export type TableDescriptor = {
  layerName: string;
  tableSchema: string;
  tableName: string;
  columnTypes: { key: string; col: string; db_type: string }[];
  requiresPromoteToMulti: boolean;
  requiresForcedPostGisDimension: boolean;
  promoteToMulti: boolean;
  forcePostGisDimension: boolean;
  // Common IFF promoteToMulti & forcePostGisDimension if either required
  postGisGeometryType: PostGisGeometryTypes;
};

export enum DatasetTypes {
  ESRI_Shapefile = "ESRI_Shapefile",
  FileGDB = "FileGDB",
  GPKG = "GPKG",
  "GeoJSON" = "GeoJSON",
}

// https://gdal.org/drivers/vector/pg.html#layer-creation-options
// * LAUNDER: This may be “YES” to force new fields created on this layer to
//        have their field names “laundered” into a form more compatible with
//        PostgreSQL. This converts to lower case and converts some special
//        characters like “-” and “#” to “_”. If “NO” exact names are preserved.
//        The default value is “YES”. If enabled the table (layer) name will also be
//        laundered.
export const launderLayerName = _.snakeCase.bind(_);
export const launderFieldName = _.snakeCase.bind(_);

export type GeoDatasetLayerFieldMetadata = {
  ignored: boolean;
  precision: number;
  width: number;
  type: string;
  name: string;
};

export type GeoDatasetLayerMetadata = {
  layerName: string;
  layerId: number;
  featuresCount: number;
  srsAuthorityName: string | null;
  srsAuthorityCode: string | null;
  fieldsMetadata: GeoDatasetLayerFieldMetadata[];
};

// Omit GIS Dataset fields by setting column to falsy value.
export function getIncludedColumnTypes(
  columnTypes: TableDescriptor["columnTypes"]
) {
  return columnTypes.filter(({ col }) => Boolean(col));
}

export function getLayersMetadata(
  datasetPath: string
): GeoDatasetLayerMetadata[] {
  const dataset = gdal.open(datasetPath);

  const layersMetadata = dataset.layers.map((layer, layerId) => {
    const { name: layerName, features, fields, srs, geomType } = layer;

    const sType = simplifyOGRwkbGeometryType(geomType);

    if (sType === OGRwkbGeometryType.wkbNone) {
      return null;
    }
    const featuresCount = features.count();

    const fieldsMetadata: GeoDatasetLayerFieldMetadata[] = fields.map(
      (fieldDefn) => {
        const { ignored, precision, width, type, name } = fieldDefn;

        return {
          ignored,
          precision,
          width,
          type,
          name,
        };
      }
    );

    const srsAuthorityName = srs?.getAuthorityName(null) ?? null;
    const srsAuthorityCode = srs?.getAuthorityCode(null) ?? null;

    // NOTE: We don't get the geometry type via metadata because it is unreliable.
    return {
      layerName,
      layerId,
      featuresCount,
      srsAuthorityName,
      srsAuthorityCode,
      fieldsMetadata,
    };
  }).filter(Boolean);

  dataset.close();

  // @ts-ignore
  return layersMetadata;
}

export type GeoDatasetMetadata = {
  GEODATASET_METADATA_VERSION: string;
  layers: GeoDatasetLayerMetadata[];
};

export async function getGeoDatasetMetadata(
  datasetPath: string
): Promise<GeoDatasetMetadata> {
  return {
    GEODATASET_METADATA_VERSION,
    layers: getLayersMetadata(datasetPath),
  };
}

export async function* makeLayerFeaturesAsyncIterator(
  datasetPath: string,
  layerName: string,
  allAsText = false // Used for analyzeSchema because OGR types such as datetime cause issues.
): AsyncGenerator<gdal.Feature> {
  const dataset = gdal.open(datasetPath);

  let layer = dataset.layers.get(layerName);

  // Convert all field values to TEXT. NOTE: Parallels loadTableSql.
  if (allAsText) {
    const fieldNames = layer.fields.getNames();
    const placeholders: string[] = [];
    const args: string[] = [];
    for (const f of fieldNames) {
      placeholders.push("CAST(%I AS character(0)) AS %I");
      args.push(f, f);
    }

    const sql = pgFormat(
      `SELECT ${placeholders.join(", ")} FROM %I`,
      ...args,
      layerName
    );

    layer = await dataset.executeSQLAsync(sql);
  }

  // const { features = null } = dataset.layers.get(layerName) || {};
  const { features = null } = layer;

  if (!features) {
    throw new Error(`Layer ${layerName} not found in dataset`);
  }

  let feature: gdal.Feature;

  while ((feature = features.next())) {
    yield feature;

    await new Promise((resolve) => process.nextTick(resolve));
  }

  dataset.close();
}

export async function* makeLayerFeatureProperiesAsyncIterator(
  datasetPath: string,
  layerName: string,
  allAsText = false
): AsyncGenerator<Record<string, any>> {
  const featuresIter = makeLayerFeaturesAsyncIterator(
    datasetPath,
    layerName,
    allAsText
  );

  for await (const feature of featuresIter) {
    const properties = feature.fields.toObject();
    yield properties;
  }
}

export async function* makeLayerFeatureGeometriesAsyncIterator(
  datasetPath: string,
  layerName: string
): AsyncGenerator<GeoJSON.Geometry> {
  const featuresIter = makeLayerFeaturesAsyncIterator(datasetPath, layerName);

  for await (const feature of featuresIter) {
    yield <GeoJSON.Geometry>feature.getGeometry().toObject();
  }
}

export type LayerFeaturesGeometryTypeCounts = Record<
  OGRwkbGeometryType, // Not entirely sure that this is the type
  number
>;

export async function getLayerFeaturesCountByGeometryType(
  datasetPath: string,
  layerName: string
): Promise<Record<OGRwkbGeometryType, number>> {
  const featuresIter = makeLayerFeaturesAsyncIterator(datasetPath, layerName);

  const countsByWkbType = <Record<OGRwkbGeometryType, number>>{};

  for await (const feature of featuresIter) {
    try {
      const { wkbType } = feature.getGeometry();

      countsByWkbType[wkbType] = countsByWkbType[wkbType] || 0;
      ++countsByWkbType[wkbType];
    } catch (err) {
      console.warn("----- WARNING -----");
      console.warn(
        "Skipping feature",
        feature.fid,
        "due to the following error"
      );
      console.warn(err);
    }
  }

  return countsByWkbType;
}

export type LayerFeaturesGeometryTypesAnalysis = {
  featuresCount: number;
  countsByWkbType: Record<OGRwkbGeometryType, number>;
  countsByPostGisDimension: Record<PostGisDimension, number>;
  countsByGeometryName: Record<string, number>;
  countsByPostGisType: Record<PostGisGeometryTypes, number>;
  minimumCommonPostgresDimension: PostGisDimension;
  requiresForcedPostGisDimension: boolean;
  typesAfterPromotingSingleGeometriesToExistingCollections: OGRwkbGeometryType[];
  requiresPromoteToMulti: boolean;
  commonWkbGeometryType: OGRwkbGeometryType;
  commonWkbGeometryName: string;
  commonPostGisGeometryType: PostGisGeometryTypes;
};

export function analyzeLayerFeaturesCountByGeometryType(
  countsByWkbType: Record<OGRwkbGeometryType, number>
): LayerFeaturesGeometryTypesAnalysis {
  const wkbTypesSet = new Set<OGRwkbGeometryType>([
    ...Object.keys(countsByWkbType).map((t) => +t),
  ]);

  const featuresCount = [...wkbTypesSet].reduce((acc, wkbType) => {
    acc += countsByWkbType[wkbType];
    return acc;
  }, 0);

  const hasZ = Object.keys(countsByWkbType).some((t) => wkbHasZ(+t));
  const hasM = Object.keys(countsByWkbType).some((t) => wkbHasM(+t));

  const countsByPostGisType = <Record<PostGisGeometryTypes, number>>_.mapKeys(
    countsByWkbType,
    (_v: any, k: string) => {
      const eType = simplifyOGRwkbGeometryType(+k);
      const postGisType = OGRwkbGeometryTypeToPostGisType[eType];

      if (postGisType === undefined) {
        throw new Error(`Cannot map OGRwkbGeometryType To PostGisType: ${+k}.`);
      }

      return postGisType;
    }
  );

  const countsByPostGisDimension = <Record<PostGisDimension, number>>[
    ...wkbTypesSet,
  ].reduce((acc, wkbType) => {
    const dimension = getPostGisDimensionForOGRwkbGeometryType(wkbType);

    acc[dimension] = acc[dimension] || 0;
    acc[dimension] += countsByWkbType[wkbType];

    return acc;
  }, {});

  const countsByGeometryName = _.mapKeys(
    countsByWkbType,
    (_v: any, k: string) => OGRGeometryTypeToName(+k)
  );

  let minimumCommonPostgresDimension: PostGisDimension;

  if (hasZ && hasM) {
    minimumCommonPostgresDimension = PostGisDimension.XYZM;
  } else if (hasZ) {
    minimumCommonPostgresDimension = PostGisDimension.XYZ;
  } else if (hasM) {
    minimumCommonPostgresDimension = PostGisDimension.XYM;
  } else {
    minimumCommonPostgresDimension = PostGisDimension.XY;
  }

  const typesAfterPromotingSingleGeometriesToExistingCollections = [
    ...wkbTypesSet,
  ].filter((wkbType) => {
    const collection = OGR_GT_GetCollection(wkbType);

    return (
      collection === OGRwkbGeometryType.wkbUnknown ||
      !wkbTypesSet.has(collection)
    );
  });

  const commonWkbGeometryType =
    typesAfterPromotingSingleGeometriesToExistingCollections.reduce(
      (acc: OGRwkbGeometryType, wkbType) => {
        const sType = simplifyOGRwkbGeometryType(wkbType);

        if (acc === null) {
          return sType;
        }

        return OGRMergeGeometryTypes(acc, sType);
      },
      // @ts-ignore
      null
    );

  const commonWkbGeometryName = OGRGeometryTypeToName(commonWkbGeometryType);
  const commonPostGisGeometryType =
    OGRwkbGeometryTypeToPostGisType[commonWkbGeometryType];

  const requiresPromoteToMulti = PostGisRootGeometryTypes.has(
    commonPostGisGeometryType
  )
    ? false // commonPostGisGeometryType accommodates single & multi
    : [...wkbTypesSet].some(
        (wkbType) =>
          // One of the types isn't in the set after promoteToMulti
          !typesAfterPromotingSingleGeometriesToExistingCollections.includes(
            wkbType
          )
      );

  // TODO: Investigate if Geometry can handle GeometryM, GeometryZ, or GeometryZM
  const requiresForcedPostGisDimension =
    featuresCount !== countsByPostGisDimension[minimumCommonPostgresDimension];

  return {
    featuresCount,
    countsByWkbType,
    countsByPostGisDimension,
    countsByGeometryName,
    countsByPostGisType,
    minimumCommonPostgresDimension,
    requiresForcedPostGisDimension,
    typesAfterPromotingSingleGeometriesToExistingCollections,
    requiresPromoteToMulti,
    commonWkbGeometryType,
    commonWkbGeometryName,
    commonPostGisGeometryType,
  };
}

export async function getLayerGeometriesAnalysis(
  datasetPath: string,
  layerName: string
) {
  const countsByWkbType = await getLayerFeaturesCountByGeometryType(
    datasetPath,
    layerName
  );

  const layerGeometriesAnalysis =
    analyzeLayerFeaturesCountByGeometryType(countsByWkbType);

  return layerGeometriesAnalysis;
}

export async function analyzeGeoDatasetLayer(
  datasetPath: string,
  layerName: string
) {
  const dataset = gdal.open(datasetPath);
  const layer = dataset.layers.get(layerName);

  if (!layer) {
    throw new Error(`Layer ${layerName} not found in dataset`);
  }

  dataset.close();

  const iter = makeLayerFeatureProperiesAsyncIterator(
    datasetPath,
    layerName,
    true // All feature field values as text
  );

  const layerFieldsAnalysis = await analyzeSchema(iter);
  const layerGeometriesAnalysis = await getLayerGeometriesAnalysis(
    datasetPath,
    layerName
  );

  return {
    GEODATASET_ANALYSIS_VERSION,
    layerFieldsAnalysis,
    layerGeometriesAnalysis,
  };
}

export function generateCreateTableStatement({
  tableSchema,
  tableName,
  columnTypes,
  postGisGeometryType,
}: TableDescriptor) {
  const includedColumnTypes = getIncludedColumnTypes(columnTypes);

  const colDefValues = includedColumnTypes.reduce(
    (acc: string[], { col, db_type }) => {
      acc.push(col);
      acc.push(db_type);
      return acc;
    },
    []
  );

  colDefValues.push(
    "wkb_geometry",
    `public.geometry(${postGisGeometryType}, 4326)`
  );

  const colsDefPlaceholders = _.chunk(colDefValues, 2)
    .map(() => "%I\t\t%s")
    .join(",\n        ");

  const sql = dedent(
    pgFormat(
      `
        CREATE SCHEMA IF NOT EXISTS %I ;

        DROP TABLE IF EXISTS %I.%I ;

        CREATE TABLE %I.%I (
          ogc_fid\t\tINTEGER PRIMARY KEY,
          ${colsDefPlaceholders}
        ) WITH (fillfactor=100, autovacuum_enabled=off) ;
      `,
      tableSchema,
      tableSchema,
      tableName,
      tableSchema,
      tableName,
      ...colDefValues
    )
  );

  return sql;
}

export function generateTempTableStatement({
  tableSchema,
  tableName,
  columnTypes,
  postGisGeometryType,
}: TableDescriptor) {
  const includedColumnTypes = getIncludedColumnTypes(columnTypes);

  const cols = includedColumnTypes.map(({ col }) => col);

  const colsDefPlaceholders = includedColumnTypes
    .map(({ db_type }) => `%I\t\t${db_type === "BYTEA" ? "BYTEA" : "TEXT"}`)
    .join(",\n        ");

  const sql = dedent(
    pgFormat(
      `
        CREATE SCHEMA IF NOT EXISTS %I ;

        CREATE TABLE %I.%I (
          ogc_fid\t\tINTEGER PRIMARY KEY,
          ${colsDefPlaceholders},
          wkb_geometry public.geometry(${postGisGeometryType}, 4326)
        ) WITH (fillfactor=100, autovacuum_enabled=off) ;
      `,
      tableSchema,
      tableSchema,
      tableName,
      ...cols
    )
  );

  return sql;
}

export function generateLoadTableStatement({
  layerName,
  columnTypes,
}: TableDescriptor) {
  const selectClauses: string[] = [];
  const placeValues: string[] = [];

  const includedColumnTypes = getIncludedColumnTypes(columnTypes);

  for (const { key, col, db_type } of includedColumnTypes) {
    // We cast ALL fields to TEXT so we can use PostgreSQL to convert them to the
    //   final column types.
    // https://gdal.org/user/ogr_sql_dialect.html#changing-the-type-of-the-fields
    //   Specifying the field_length and/or the field_precision is optional. An
    //   explicit value of zero can be used as the width for CHARACTER() to
    //   indicate variable width.
    if (db_type === "BYTEA") {
      selectClauses.push("%I AS %I");
    } else {
      selectClauses.push("CAST(%I AS CHARACTER(0)) AS %I");
    }
    placeValues.push(key, col);
  }

  const colsDefPlaceholders = selectClauses.join(",\n            ");

  const sql = dedent(
    pgFormat(
      `
        SELECT
            ${colsDefPlaceholders}
          FROM %I
      `,
      ...placeValues,
      layerName
    )
  );

  return sql;
}

export async function loadTable(
  datasetPath: string,
  tableDescriptor: TableDescriptor,
  pgEnv: string,
  // tableDescriptor MUST be sufficient to geneate the CREATE and load SQL,
  // but we MUST also support manual edits to the createTableSql and loadTableSql
  // for inevitable edge-cases that tableDescriptor/sqlGenerators don't yet handle.
  createTableSqlPath?: string,
  loadTableSqlPath?: string
) {
  if (createTableSqlPath && !existsSync(createTableSqlPath)) {
    throw new Error(
      `createTableSqlPath file does not exist: ${createTableSqlPath}`
    );
  }

  if (loadTableSqlPath && !existsSync(loadTableSqlPath)) {
    throw new Error(`loadTableSql file does not exist: ${loadTableSqlPath}`);
  }

  console.time('load table create table sql')
  const createTableSql = createTableSqlPath
    ? await readFileAsync(createTableSqlPath)
    : generateCreateTableStatement(tableDescriptor);
  


  const loadTableSql = loadTableSqlPath
    ? await readFileAsync(loadTableSqlPath)
    : generateLoadTableStatement(tableDescriptor);

  const connStr = getPostgresConnectionString(pgEnv);
  console.timeEnd('load table create table sql')

  let success: Function;
  let fail: Function;

  const done = new Promise((resolve, reject) => {
    success = resolve;
    fail = reject;
  });

  // The TEMPORY table uses the TEXT data type for all column besides ogc_fid and wkb_geometry.
  // This is so we can better control how the values are handled using the CLOSING_STATEMENTS.
  console.time('loadTable generate tmp sql')
  const tempTableDescriptor = _.cloneDeep(tableDescriptor);
  const tstamp = new Date().toISOString().replace(/[^0-9a-z]/gi, "");
  tempTableDescriptor.tableName = `staging_${tstamp}`;
  const createTempTableSql = generateTempTableStatement(tempTableDescriptor);

  const loadTempTableSql = generateLoadTableStatement(tableDescriptor);
  console.timeEnd('loadTable generate tmp sql')
  
  console.time('loadTable temptable')
  
  // Because of quotations, it's easier to use a file for the load sql.
  const { loadTempTableSqlFilePath, rmLoadTempTableSqlFile } =
    await new Promise((resolve, reject) => {
      tmp.file(async (err, path, _fd, cleanupCallback) => {
        if (err) {
          return reject(err);
        }

        return resolve({
          loadTempTableSqlFilePath: path,
          rmLoadTempTableSqlFile: cleanupCallback,
        });
      });
    });
  console.timeEnd('loadTable temptable')

  console.time('loadTable write table temptable')

  await writeFileAsync(loadTempTableSqlFilePath, loadTempTableSql);
  console.timeEnd('loadTable write table temptable')

  try {
    const {
      tableSchema,
      tableName,
      columnTypes,
      promoteToMulti,
      forcePostGisDimension,
    } = tableDescriptor;

    const PRELUDE_STATEMENTS = ["BEGIN;", createTempTableSql].join("\n\n");

    const includedColumnTypes = getIncludedColumnTypes(columnTypes);

    const cols = includedColumnTypes.map(({ col }) => col);
    const colsHolders = cols.map(() => "%I");
    const colCasts: string[] = [];
    const colCastsUsing: string[] = [];

    includedColumnTypes.forEach(({ col, db_type }) => {
      if (db_type === "BYTEA") {
        colCasts.push("%I");
        colCastsUsing.push(col);
      } else if (
        db_type === PgDataType.BOOLEAN ||
        pgIntegerTypes.includes(<PgDataType>db_type)
      ) {
        colCasts.push(
          `CAST(
                NULLIF(
                  REGEXP_REPLACE(
                    TRIM(%I),
                    '\\.0{1,}$',
                    ''
                  ),
                  ''
                )
                AS %s
              ) AS %I`
        );
        colCastsUsing.push(col, db_type, col);
      } else {
        colCasts.push("CAST( NULLIF( TRIM(%I), '' ) AS %s ) AS %I");
        colCastsUsing.push(col, db_type, col);
      }
    });

    const tmpToTableSql = dedent(
      pgFormat(
        `
          INSERT INTO %I.%I (ogc_fid, ${colsHolders.join(", ")}, wkb_geometry)
            SELECT
                ogc_fid as ogc_fid,
                ${colCasts.join(",\n              ")},
                wkb_geometry
              FROM %I.%I ;
        `,
        tableDescriptor.tableSchema,
        tableDescriptor.tableName,
        ...cols,
        ...colCastsUsing,
        tempTableDescriptor.tableSchema,
        tempTableDescriptor.tableName
      )
    );

    // After loading, we convert the BOOLEAN columns back from TEXT.
    const CLOSING_STATEMENTS = [
      createTableSql,
      tmpToTableSql,
      pgFormat(
        "DROP TABLE %I.%I;",
        tempTableDescriptor.tableSchema,
        tempTableDescriptor.tableName
      ),

      pgFormat(
        "CREATE INDEX %I ON %I.%I USING GIST(wkb_geometry);",
        `${tableName}_gix`,
        tableSchema,
        tableName
      ),
      // TOD0: Limit length of tableName so normal autogenerated index name.
      pgFormat(
        "CLUSTER %I.%I USING %I_pkey;",
        tableSchema,
        tableName,
        tableName
      ),
      pgFormat("ANALYZE %I.%I;", tableSchema, tableName),
      "COMMIT;",
    ].join(" ");

    const promoteToMultiArgs = promoteToMulti
      ? ["-nlt", "PROMOTE_TO_MULTI"]
      : [];
    const forcePostGisDimensionArgs = forcePostGisDimension
      ? ["-dim", "XYM"]
      : [];

    const ogr2ogrArgs = [
      // "-limit",
      // "10",
      "-preserve_fid",
      "-doo",
      `PRELUDE_STATEMENTS=${PRELUDE_STATEMENTS}`,
      "-doo",
      `CLOSING_STATEMENTS=${CLOSING_STATEMENTS}`,
      "-skipfailures",
      "-append",
      // "-emptyStrAsNull", // Need ogr2ogr version 3.3
      "-t_srs",
      "EPSG:4326",
      "--config",
      "OGR_TRUNCATE",
      "YES",
      "--config",
      "PG_USE_COPY",
      "YES",
      ...promoteToMultiArgs,
      ...forcePostGisDimensionArgs,
      "-nln",
      `${tempTableDescriptor.tableSchema}.${tempTableDescriptor.tableName}`,
      "-sql",
      `@${loadTempTableSqlFilePath}`,
      datasetPath,
    ];

    console.time('loadTable ogr2ogrLoad')
    const ogr2ogrLoad = spawn(
      "ogr2ogr",
      ["-F", "PostgreSQL", `PG:${connStr}`, ...ogr2ogrArgs],
      {
        stdio: "pipe",
      }
    )
      // @ts-ignore
      .once("error", fail)
      // @ts-ignore
      .once("close", success);

    let loadStdOut = "";

    /* eslint-disable-next-line no-unused-expressions */
    ogr2ogrLoad.stdout?.on("data", (data) => {
      console.log('ogr', data.toString())
      process.stdout.write(data);
      loadStdOut += data.toString();
    });

    let loadStdErr = "";

    /* eslint-disable-next-line no-unused-expressions */
    ogr2ogrLoad.stderr?.on("data", (data) => {
      process.stderr.write(data);
      loadStdErr += data.toString();
    });

    await done;

    console.timeEnd('loadTable ogr2ogrLoad')

    return {
      tableSchema,
      tableName,
      tableDescriptor,
      createTableSql,
      loadTableSql,
      ogr2ogrArgs,
      loadStdOut,
      loadStdErr,
    };
  } catch (err) {
    throw err;
  } finally {
    rmLoadTempTableSqlFile();
  }
}
