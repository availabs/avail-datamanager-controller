import { execSync } from "child_process";
import fs from "fs";
import { PoolClient } from "pg";
import pgFormat from "pg-format";
import dedent from "dedent";
import logger from "data_manager/logger";
import dama_meta from "data_manager/meta";
import { getEtlContextId } from "data_manager/contexts";

import { getPostgresConnectionString } from "../../../data_manager/dama_db/postgres/PostgreSQL";

// eslint-disable-next-line @typescript-eslint/no-var-requires
const shapefileToGeojson = require("shapefile-to-geojson");

export async function createView(
  view_values: Record<string, string | number | any>,
  dbConnection: PoolClient
) {
  const {
    source_id,
    user_id,
    customViewAttributes,
    viewMetadata,
    viewDependency,
  } = view_values;

  let newDamaView = await dama_meta.createNewDamaView({
    user_id,
    source_id,
    etl_context_id: getEtlContextId(),
    view_dependencies: viewDependency,
    metadata: { ...(customViewAttributes || {}), ...(viewMetadata || {}) },
    table_name: view_values?.table_name,
  });

  const {
    view_id: damaViewId,
    table_schema: origTableSchema,
    table_name: origTableName,
    metadata,
  } = newDamaView;

  const table_schema = origTableSchema || "geo";
  let table_name = `${view_values?.table_name}_${damaViewId}`;

  // Assign the default table_name if one wasn't specified
  if (!origTableName) {
    const text =
      "SELECT _data_manager_admin.dama_view_name($1) AS dama_view_name;";

    const {
      rows: [{ dama_view_name }],
    } = await dbConnection.query({
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

    dbConnection.query(q);

    const { rows } = await dbConnection.query({
      text: "SELECT * FROM data_manager.views WHERE ( view_id = $1 );",
      values: [damaViewId],
    });

    newDamaView = rows[0];
  }

  return newDamaView;
}

export const fetchFileList = async (
  fileName: string,
  url: string,
  table_name: string,
  tmpLocation: string,
  cnt: number,
): Promise<void> => {
  console.log("fetching...", url + fileName);
  if (!fs.existsSync(tmpLocation)) {
    fs.mkdirSync(tmpLocation);
  }

  const tmpLocationDownload = `${tmpLocation}/${fileName}`;
  const tmpLocationProcess = `${tmpLocation}/tmp_dir`;

  execSync(`curl -o ${tmpLocationDownload} '${url}${fileName}'`);
  execSync(`rm -rf ${tmpLocationProcess}`);
  execSync(`mkdir -p ${tmpLocationProcess}`);
  execSync(`unzip -d ${tmpLocationProcess} ${tmpLocationDownload}`, {
    encoding: "utf-8",
  });

  const geoJSON = await shapefileToGeojson.parseFiles(
    `${tmpLocationProcess}/${fileName.replace(".zip", ".shp")}`,
    `${tmpLocationProcess}/${fileName.replace(".zip", ".dbf")}`
  );

  if (cnt === 0) {
    logger.info(`geoJSON is \n\n: ${JSON.stringify(geoJSON, null, 3)}`);
  }
  // logger.info(`\nMore logs about file ${tmpLocation}/${fileName}`);
  geoJSON.features = geoJSON?.features?.map((f: any) => {
    f.properties.geoid = f?.properties?.GEOID;
    f.properties.name = f?.properties?.NAMELSAD;
    return f;
  });

  fs.writeFileSync(
    `tmp-etl/tl_2017_${table_name}/${fileName.replace(".zip", ".json")}`,
    JSON.stringify(geoJSON),
    "utf8"
  );

  execSync(`rm -rf ${tmpLocationProcess}`);
};

export const uploadFiles = (
  fileName: string,
  pgEnv = "tig_dama_dev",
  url: string,
  table_name: string,
  view_id: number,
  tmpLocation: any
) => {
  logger.info(`uploading... ${url + fileName}`);

  const pg = getPostgresConnectionString(pgEnv);
  execSync(
    `ogr2ogr -f PostgreSQL PG:"${pg} schemas=geo" ${fileName.replace(
      ".zip",
      ".json"
    )} -lco GEOMETRY_NAME=wkb_geometry -lco GEOM_TYPE=geometry -t_srs EPSG:4326 -lco FID=gid -lco SPATIAL_INDEX=GIST -nlt PROMOTE_TO_MULTI -overwrite ${
      ["state", "county"].includes(table_name?.toLowerCase())
        ? `-nln geo.tl_2017_${table_name}_${view_id}`
        : ""
    }`,
    { cwd: `./${tmpLocation}` }
  );

  logger.info(
    `Here is the command :\n\n ogr2ogr -f PostgreSQL PG:"${pg} schemas=geo" ${fileName.replace(
      ".zip",
      ".json"
    )} -lco GEOMETRY_NAME=wkb_geometry -lco GEOM_TYPE=geometry -t_srs EPSG:4326 -lco FID=gid -lco SPATIAL_INDEX=GIST -nlt PROMOTE_TO_MULTI -overwrite ${
      ["state", "county"].includes(table_name.toLowerCase())
        ? `-nln geo.tl_2017_${table_name}_${view_id}`
        : ""
    }`,
    { cwd: `./${tmpLocation}` }
  );
  execSync(`rm -rf ${tmpLocation}/${fileName.replace(".zip", ".json")}`);
};

export const mergeTables = async (
  fileNames: Array<string>,
  view_id: number,
  table_name: string,
  dbConnection: PoolClient
) => {
  let sql = fileNames
    ?.map(
      (file) =>
        `SELECT wkb_geometry, statefp, countyfp, geoid2 as geoid, name2 as name, namelsad FROM geo.${file} `
    )
    .join(` UNION ALL `);

  sql = `with t as ( ${sql} )
        SELECT * INTO geo.tl_2017_${table_name}_${view_id} FROM t;`;

  logger.info(`\nSQL is: \n${sql}`);

  return dbConnection.query(sql);
};

export const dropTmpTables = async (
  fileNames: Array<string | number>,
  dbConnection: PoolClient
) => {
  let sql = fileNames
    ?.map((file: string | any) => `DROP TABLE IF Exists geo.${file};`)
    .join(" ");

  logger.info(`sql here in the dropTmpTables: \n\n\n ${sql}`);
  sql = `BEGIN;
          ${sql}
        COMMIT;`;

  return dbConnection.query(sql);
};

export const createIndices = async (
  view_id: number,
  table_name: string,
  sqlLog: Array<string>,
  dbConnection: PoolClient
): Promise<void> => {
  const query = `
      BEGIN;
        CREATE INDEX IF NOT EXISTS wkb_geometry_idx_tl_2017_${table_name}_${view_id}
        ON geo.tl_2017_${table_name}_${view_id} USING gist
        (wkb_geometry)
        TABLESPACE pg_default;
      COMMIT;
    `;

  logger.info(`Query for the create indices is: \n ${query}`);
  sqlLog.push(query);

  dbConnection.query(query);
};

export const correctGeoid = async (
  view_id: number,
  table_name: string,
  sqlLog: Array<string>,
  dbConnection: PoolClient
) => {
  const geoLengths = {
    state: 2,
    county: 5,
    cousub: 10,
    tract: 11,
  };

  const query: string = `
      BEGIN;

      ALTER TABLE geo.tl_2017_${table_name}_${view_id}
      ALTER COLUMN geoid TYPE text USING lpad(geoid::text, ${geoLengths[table_name]}, '0');

      COMMIT;
    `;

  logger.info(`Query for the correctGeoid is: \n ${query}`);
  sqlLog.push(query);

  return dbConnection.query(query);
};