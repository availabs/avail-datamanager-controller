import { execSync } from "child_process";
import fs from "fs";
import { PoolClient, QueryConfig, QueryResult } from "pg";
import { Context } from "moleculer";
import { FSA } from "flux-standard-action";
import dedent from "dedent";
import pgFormat from "pg-format";
import EventTypes from "../constants/EventTypes";
import { createViewMbtiles } from "../../dt-gis_dataset/mbtiles/mbtiles";
import { getPostgresConnectionString } from "../../../data_manager/dama_db/postgres/PostgreSQL";
import { getFiles } from "./scrapper";
import { err, fin, init, update_view } from "../utils/macros";

// eslint-disable-next-line @typescript-eslint/no-var-requires
const shapefileToGeojson = require("shapefile-to-geojson");

const fetchFileList = async (fileName, url, table_name, tmpLocation) => {
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
  geoJSON.features = geoJSON.features.map((f) => {
    f.properties.geoid = f.properties.GEOID;
    f.properties.name = f.properties.NAMELSAD;
    return f;
  });

  fs.writeFileSync(
    `tmp-etl/tl_2017_${table_name}/${fileName.replace(".zip", ".json")}`,
    JSON.stringify(geoJSON),
    "utf8"
  );

  execSync(`rm -rf ${tmpLocationProcess}`);
};

const uploadFiles = (
  fileName,
  pgEnv = "dama_dev_1",
  url,
  table_name,
  view_id,
  tmpLocation
) => {
  console.log("uploading...", url + fileName);

  const pg = getPostgresConnectionString(pgEnv);
  execSync(
    `ogr2ogr -f PostgreSQL PG:"${pg} schemas=geo" ${fileName.replace(
      ".zip",
      ".json"
    )} -lco GEOMETRY_NAME=geom -lco GEOM_TYPE=geometry -t_srs EPSG:4326 -lco FID=gid -lco SPATIAL_INDEX=GIST -nlt PROMOTE_TO_MULTI -overwrite ${
      ["state", "county"].includes(table_name.toLowerCase())
        ? `-nln geo.tl_2017_${table_name}_${view_id}`
        : ``
    }`,
    { cwd: `./${tmpLocation}` }
  );

  execSync(`rm -rf ${tmpLocation}/${fileName.replace(".zip", ".json")}`);
};

const mergeTables = async (ctx, fileNames, view_id, table_name) => {
  let sql = fileNames
    .map(
      (file) =>
        `SELECT geom, statefp, countyfp, geoid2 as geoid, name2 as name, namelsad FROM geo.${file} `
    )
    .join(` UNION ALL `);

  sql = `with t as (
                    ${sql}
                    )

        SELECT * INTO geo.tl_2017_${table_name}_${view_id} FROM t;`;

  return ctx.call("dama_db.query", {
    text: sql,
  });
};

const dropTmpTables = async (ctx, fileNames) => {
  let sql = fileNames.map((file) => `DROP TABLE geo.${file};`).join(` `);

  sql = `BEGIN;
          ${sql}
          COMMIT;`;

  return ctx.call("dama_db.query", {
    text: sql,
  });
};

const createIndices = async (ctx, view_id, table_name, sqlLog) => {
  let query = `
      BEGIN;
      CREATE INDEX IF NOT EXISTS geom_idx_tl_2017_${table_name}_${view_id}
      ON geo.tl_2017_${table_name}_${view_id} USING gist
      (geom)
      TABLESPACE pg_default;

    COMMIT;
    `;
  sqlLog.push(query);
  return ctx.call("dama_db.query", {
    text: query,
  });
};

const correctGeoid = async (ctx, view_id, table_name, sqlLog) => {
  const geoLengths = {
    state: 2,
    county: 5,
    cousub: 10,
    tract: 11,
  };
  let query = `
      BEGIN;

      ALTER TABLE geo.tl_2017_${table_name}_${view_id}
      ALTER COLUMN geoid TYPE text USING lpad(geoid::text, ${geoLengths[table_name]}, '0');

      COMMIT;
    `;
  sqlLog.push(query);
  return ctx.call("dama_db.query", {
    text: query,
  });
};
export default async function publish(ctx: Context) {
  const {
    // @ts-ignore
    params: { table_name },
    // @ts-ignore
    meta: { pgEnv },
  } = ctx;
  const url = `https://www2.census.gov/geo/tiger/TIGER2017/${table_name}/`;

  console.log("---- Reached here 1 ----");

  const { etl_context_id, dbConnection, source_id, view_id, sqlLog } =
    await init({ ctx, type: `tl_${table_name.toLowerCase()}` });

  console.log("---- Reached here 2 ----");
  try {
    const tmpLocation = `tmp-etl/tl_2017_${table_name}`;

    let files = await getFiles(url);
    console.log("---- Reached here 3 ----");

    const sliceVar = 3;
    if (files.length > sliceVar) {
      files = files.slice(0, sliceVar);
    }
    await files.reduce(async (acc, curr) => {
      await acc;
      return fetchFileList(curr, url, table_name, tmpLocation).then(() =>
        uploadFiles(curr, pgEnv, url, table_name, view_id, tmpLocation)
      );
    }, Promise.resolve());
    console.log("---- Reached here 4 ----");
    execSync(`rm -rf ${tmpLocation}`);

    if (files.length > 1) {
      console.log("---- Reached here 5 ----");
      await mergeTables(
        ctx,
        files.map((f) => f.replace(".zip", "")),
        view_id,
        table_name
      );

      console.log("---- Reached here 6 ----");
      await dropTmpTables(
        ctx,
        files.map((f) => f.replace(".zip", ""))
      );
    }

    console.log("---- Reached here 7 ----");

    await createIndices(ctx, view_id, table_name.toLowerCase(), sqlLog);

    console.log("---- Reached here 8 ----");
    await correctGeoid(ctx, view_id, table_name.toLowerCase(), sqlLog);

    console.log("---- Reached here 9 ----");
    await update_view({
      table_schema: "geo",
      table_name: `tl_2017_${table_name.toLowerCase()}`,
      view_id,
      dbConnection,
      sqlLog,
    });

    console.log("---- Reached here 10 ----");
    await createViewMbtiles(view_id, source_id, etl_context_id);
    console.log("---- Reached here 11 ----");
    return fin({
      etl_context_id,
      ctx,
      dbConnection,
      payload: { view_id, source_id },
    });
  } catch (e) {
    console.log("---- Reached here 12 ----");
    return err({ e, etl_context_id, sqlLog, ctx, dbConnection });
  }
}
