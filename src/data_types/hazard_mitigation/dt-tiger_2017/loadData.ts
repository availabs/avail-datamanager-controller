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

// eslint-disable-next-line @typescript-eslint/no-var-requires
const shapefileToGeojson = require("shapefile-to-geojson");

export const ReadyToPublishPrerequisites = [
  EventTypes.QA_APPROVED,
  EventTypes.VIEW_METADATA_SUBMITTED,
];

// mol $ call 'dama/data_source_integrator.testDownloadAction' --table_name details --#pgEnv dama_dev_1

const fetchFileList = async (fileName, url, table) => {
  console.log("fetching...", url + fileName);
  if(!fs.existsSync(`tl_2017_${table}/`)){
    fs.mkdirSync(`tl_2017_${table}/`);
  }
  const file_path = "data/" + `tl_2017_${table}/` + fileName;

  execSync(`curl -o ${fileName} '${url}${fileName}'`);
  execSync(`rm -rf data/tl_2017_${table}/tmp_dir`);
  execSync(`mkdir -p data/tl_2017_${table}/tmp_dir`);
  execSync(`unzip -d data/tl_2017_${table}/tmp_dir ${fileName}`, { encoding: "utf-8" });

  const geoJSON = await shapefileToGeojson.parseFiles(`data/tl_2017_${table}/tmp_dir/${fileName.replace(".zip", ".shp")}`, `data/tl_2017_${table}/tmp_dir/${fileName.replace(".zip", ".dbf")}`);
  geoJSON.features = geoJSON.features.map(f => {
    f.properties.geoid = f.properties.GEOID;
    f.properties.name = f.properties.NAMELSAD;
    return f;
  })

  fs.writeFileSync(`data/tl_2017_${table}/${fileName.replace(".zip", ".json")}`, JSON.stringify(geoJSON), "utf8");

  execSync(`rm -rf data/tl_2017_${table}/tmp_dir`);
};

const uploadFiles = (fileName, pgEnv="dama_dev_1", url, table, view_id) => {
  console.log("uploading...", url + fileName)

  const pg = getPostgresConnectionString(pgEnv);
  execSync(`ogr2ogr -f PostgreSQL PG:"${pg} schemas=geo" ${fileName.replace(".zip", ".json")} -lco GEOMETRY_NAME=geom -lco GEOM_TYPE=geometry -t_srs EPSG:4326 -lco FID=gid -lco SPATIAL_INDEX=GIST -nlt PROMOTE_TO_MULTI -overwrite ${["state", "county"].includes(table.toLowerCase()) ? `-nln geo.tl_2017_${table}_${view_id}` : ``}`,
    {cwd: `./data/tl_2017_${table}/`}
  ) // -nln tl_2017_${table}

  execSync(`rm -rf ./data/tl_2017_${table}/${fileName.replace(".zip", ".json")}`);
};

async function getInsertViewMetadataSql(
  ctx: Context,
  viewMetadataSubmittedEvent: FSA
) {
  const insertViewMetaSql = <QueryConfig>(
    await ctx.call(
      "dama/metadata.getInsertDataManagerViewMetadataSql",
      viewMetadataSubmittedEvent
    )
  );

  return insertViewMetaSql;
}

const create_view = async (etl_context_id, ctx) => {
  // insert into views, get view id, and use it in table name.

  const events: FSA[] = await ctx.call("dama_dispatcher.queryDamaEvents", {
    etl_context_id,
  });

  const eventByType = events.reduce((acc, damaEvent: FSA) => {
    acc[damaEvent.type] = damaEvent;
    return acc;
  }, {});

  const viewMetadataSubmittedEvent =
    eventByType[EventTypes.VIEW_METADATA_SUBMITTED];

  const insertViewMetaSql = await getInsertViewMetadataSql(
    ctx,
    viewMetadataSubmittedEvent
  );

  const res = await ctx.call("dama_db.query", insertViewMetaSql);

  const {
    rows: [viewMetadata],
  } = res;

  return viewMetadata

}

const update_view = async (view_id, ctx, table) => {
  // update view meta
  const updateViewMetaSql = dedent(
    `
        UPDATE data_manager.views
          SET
            table_schema  = $1,
            table_name    = $2,
            data_table    = $3
          WHERE view_id = $4
      `
  );

  const data_table = pgFormat("%I.%I", "geo", `tl_2017_${table}_${view_id}`);

  return ctx.call("dama_db.query", {
    text: updateViewMetaSql,
    values: ["geo", `tl_2017_${table}_${view_id}`, data_table, view_id],
  });

};

const mergeTables = async (ctx, fileNames, view_id, table) => {
  let sql = fileNames.map(file =>
    `SELECT geom, statefp, countyfp, geoid2 as geoid, name2 as name, namelsad FROM geo.${file} `
    ).join(` UNION ALL `);

  sql = `with t as (
                    ${sql}
                    )

        SELECT * INTO geo.tl_2017_${table}_${view_id} FROM t;`;

  return ctx.call("dama_db.query", {
    text: sql,
  });
};

const createIndices = async (ctx, view_id, table) => {
  let query = `
      BEGIN;
      CREATE INDEX IF NOT EXISTS geom_idx_tl_2017_${table}_${view_id}
      ON geo.tl_2017_${table}_${view_id} USING gist
      (geom)
      TABLESPACE pg_default;

    COMMIT;
    `
  return ctx.call("dama_db.query", {
    text: query,
  });
}

const correctGeoid = async (ctx, view_id, table) => {
  let query = `
      BEGIN;

      ALTER TABLE geo.tl_2017_${table}_${view_id}
      ALTER COLUMN geoid TYPE text;

      UPDATE geo.tl_2017_${table}_${view_id} dst
      set geoid = lpad(geoid::text, ${table === "tract" ? 11 : 10}, '0')
      where length(geoid::text) = ${table === "tract" ? 10 : 9};

      COMMIT;
    `
  return ctx.call("dama_db.query", {
    text: query,
  });
}
export default async function publish(ctx: Context) {
  // throw new Error("publish TEST ERROR");

  const etlcontextid = await ctx.call(
    "dama_dispatcher.spawnDamaContext",
    { etl_context_id: null, pgEnv: null, table: null }
  );

  const {
    // @ts-ignore
    params: { etl_context_id = etlcontextid, table, src_id, view_id },
    meta: { pgEnv },
  } = ctx;
  const url = `https://www2.census.gov/geo/tiger/TIGER2017/${table}/`;

  if (!(etl_context_id)) {
    throw new Error("The etl_context_id parameter is required.");
  }

  try {
    // step 1
    const files = await getFiles(url);

    await files.reduce(async (acc, curr) => {
              await acc;
              return fetchFileList(curr, url, table).then(() => uploadFiles(curr, pgEnv, url, table, view_id));
            }, Promise.resolve());


    if (files.length > 1){
      await mergeTables(ctx, files.map(f => f.replace(".zip", "")), view_id, table);
    }

    await createIndices(ctx, view_id, table);

    await correctGeoid(ctx, view_id, table.toLowerCase());

    await update_view(view_id, ctx, table.toLowerCase());

    const finalEvent = {
      type: EventTypes.FINAL,
      payload: {
        data_manager_view_id: -1,
      },
      meta: {
        timestamp: new Date().toISOString(),
        etl_context_id
      },
    };

    await ctx.call("dama_dispatcher.dispatch", finalEvent);

    return finalEvent;
  } catch (err) {
    console.error(err);

    const errEvent = {
      type: EventTypes.PUBLISH_ERROR,
      payload: {
        message: err.message,
      },
      meta: {
        timestamp: new Date().toISOString(),
        etl_context_id
      },
    };

    await ctx.call("dama_dispatcher.dispatch", errEvent);

    throw err;
  }
}
