import {execSync} from "child_process";
import fs from "fs";
import { PoolClient, QueryConfig, QueryResult } from "pg";
import { Context } from "moleculer";
import {FSA} from "flux-standard-action";
import dedent from "dedent";
import pgFormat from "pg-format";
import EventTypes from "../constants/EventTypes";
import {getPostgresConnectionString} from "../../../data_manager/dama_db/postgres/PostgreSQL"
import {getFiles} from "./scrapper";
import {err, fin, init, update_view} from "../utils/macros";

// eslint-disable-next-line @typescript-eslint/no-var-requires
const shapefileToGeojson = require("shapefile-to-geojson");

const fetchFileList = async (fileName, url, table_name) => {
  console.log("fetching...", url + fileName);
  if(!fs.existsSync(`tl_2017_${table_name}/`)){
    fs.mkdirSync(`tl_2017_${table_name}/`);
  }
  const file_path = "data/" + `tl_2017_${table_name}/` + fileName;

  execSync(`curl -o ${fileName} '${url}${fileName}'`);
  execSync(`rm -rf data/tl_2017_${table_name}/tmp_dir`);
  execSync(`mkdir -p data/tl_2017_${table_name}/tmp_dir`);
  execSync(`unzip -d data/tl_2017_${table_name}/tmp_dir ${fileName}`, { encoding: "utf-8" });

  const geoJSON = await shapefileToGeojson.parseFiles(`data/tl_2017_${table_name}/tmp_dir/${fileName.replace(".zip", ".shp")}`, `data/tl_2017_${table_name}/tmp_dir/${fileName.replace(".zip", ".dbf")}`);
  geoJSON.features = geoJSON.features.map(f => {
    f.properties.geoid = f.properties.GEOID;
    f.properties.name = f.properties.NAMELSAD;
    return f;
  })

  fs.writeFileSync(`data/tl_2017_${table_name}/${fileName.replace(".zip", ".json")}`, JSON.stringify(geoJSON), "utf8");

  execSync(`rm -rf data/tl_2017_${table_name}/tmp_dir`);
};

const uploadFiles = (fileName, pgEnv="dama_dev_1", url, table_name, view_id) => {
  console.log("uploading...", url + fileName)

  const pg = getPostgresConnectionString(pgEnv);
  execSync(`ogr2ogr -f PostgreSQL PG:"${pg} schemas=geo" ${fileName.replace(".zip", ".json")} -lco GEOMETRY_NAME=geom -lco GEOM_TYPE=geometry -t_srs EPSG:4326 -lco FID=gid -lco SPATIAL_INDEX=GIST -nlt PROMOTE_TO_MULTI -overwrite ${["state", "county"].includes(table_name.toLowerCase()) ? `-nln geo.tl_2017_${table_name}_${view_id}` : ``}`,
    {cwd: `./data/tl_2017_${table_name}/`}
  ) // -nln tl_2017_${table_name}

  execSync(`rm -rf ./data/tl_2017_${table_name}/${fileName.replace(".zip", ".json")}`);
};

const mergeTables = async (ctx, fileNames, view_id, table_name) => {
  let sql = fileNames.map(file =>
    `SELECT geom, statefp, countyfp, geoid2 as geoid, name2 as name, namelsad FROM geo.${file} `
    ).join(` UNION ALL `);

  sql = `with t as (
                    ${sql}
                    )

        SELECT * INTO geo.tl_2017_${table_name}_${view_id} FROM t;`;

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
}

const correctGeoid = async (ctx, view_id, table_name, sqlLog) => {
  let query = `
      BEGIN;

      ALTER TABLE geo.tl_2017_${table_name}_${view_id}
      ALTER COLUMN geoid TYPE text;

      UPDATE geo.tl_2017_${table_name}_${view_id} dst
      set geoid = lpad(geoid::text, ${table_name === "tract" ? 11 : 10}, '0')
      where length(geoid::text) = ${table_name === "tract" ? 10 : 9};

      COMMIT;
    `;
  sqlLog.push(query);
  return ctx.call("dama_db.query", {
    text: query,
  });
}
export default async function publish(ctx: Context) {
  const {
    // @ts-ignore
    params: { table_name },
    meta: { pgEnv },
  } = ctx;
  const url = `https://www2.census.gov/geo/tiger/TIGER2017/${table_name}/`;

  const {etl_context_id, dbConnection, source_id, view_id, sqlLog} = await init({ctx, type: `tl_${table_name.toLowerCase()}`});

  try {
    // step 1
    const files = await getFiles(url);

    await files.reduce(async (acc, curr) => {
              await acc;
              return fetchFileList(curr, url, table_name).then(() => uploadFiles(curr, pgEnv, url, table_name, view_id));
            }, Promise.resolve());


    if (files.length > 1){
      await mergeTables(ctx, files.map(f => f.replace(".zip", "")), view_id, table_name);
    }

    await createIndices(ctx, view_id, table_name.toLowerCase(), sqlLog);

    await correctGeoid(ctx, view_id, table_name.toLowerCase(), sqlLog);

    await update_view({table_schema: 'geo', table_name: `tl_2017_${table_name.toLowerCase()}`, view_id, dbConnection, sqlLog});

    return fin({etl_context_id, ctx, dbConnection, payload: {view_id, source_id}});
  } catch (e) {
    return err({e, etl_context_id, sqlLog, ctx, dbConnection});
  }
}
