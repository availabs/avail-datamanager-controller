import { Context } from "moleculer";

import { PoolClient, QueryConfig, QueryResult } from "pg";
import {execSync} from "child_process";
import EventTypes from "../../constants/EventTypes";
import fs from "fs";
import tmp from "tmp";
import https from "https";
import {getFiles} from "./scrapper";
import {data} from "cheerio/lib/api/attributes";
import {getPostgresConnectionString} from '../../../../data_manager/dama_db/postgres/PostgreSQL'
import {createSqls} from "../upload";
import {FSA} from "flux-standard-action";
import dedent from "dedent";
import pgFormat from "pg-format";
import {tables} from "../tables";

const shapefileToGeojson = require("shapefile-to-geojson");

export const ReadyToPublishPrerequisites = [
  EventTypes.QA_APPROVED,
  EventTypes.VIEW_METADATA_SUBMITTED,
];

const url = 'https://www2.census.gov/geo/tiger/TIGER2017/COUSUB/'
// mol $ call 'dama/data_source_integrator.testDownloadAction' --table_name details --#pgEnv dama_dev_1

const fetchFileList = async (fileName) => {
  console.log('fetching...', url + fileName)
  if(!fs.existsSync("tl_2017_cousub/")){
    fs.mkdirSync("tl_2017_cousub/")
  }
  const file_path = "data/" + "tl_2017_cousub/" + fileName;

  execSync(`curl -o ${fileName} 'https://www2.census.gov/geo/tiger/TIGER2017/COUSUB/${fileName}'`);
  execSync(`rm -rf data/tl_2017_cousub/tmp_dir`);
  execSync(`mkdir -p data/tl_2017_cousub/tmp_dir`);
  execSync(`unzip -d data/tl_2017_cousub/tmp_dir ${fileName}`, { encoding: 'utf-8' });

  const geoJSON = await shapefileToGeojson.parseFiles(`data/tl_2017_cousub/tmp_dir/${fileName.replace('.zip', '.shp')}`, `data/tl_2017_cousub/tmp_dir/${fileName.replace('.zip', '.dbf')}`);
  geoJSON.features = geoJSON.features.map(f => {
    f.properties.geoid = f.properties.GEOID;
    f.properties.name = f.properties.NAMELSAD;
    return f;
  })

  fs.writeFileSync(`data/tl_2017_cousub/${fileName.replace(".zip", ".json")}`, JSON.stringify(geoJSON), "utf8");

  execSync(`rm -rf data/tl_2017_cousub/tmp_dir`);
}

const uploadFiles = (fileName, pgEnv="dama_dev_1") => {
  console.log("uploading...", url + fileName)

  const pg = getPostgresConnectionString(pgEnv);
  execSync(`ogr2ogr -f PostgreSQL PG:"${pg} schemas=geo" ${fileName.replace(".zip", ".json")} -lco GEOMETRY_NAME=geom -lco FID=gid -lco SPATIAL_INDEX=GIST -nlt PROMOTE_TO_MULTI -overwrite`,
    {cwd: "./data/tl_2017_cousub/"}
  ) // -nln tl_2017_cousub

  execSync(`rm -rf ./data/tl_2017_cousub/${fileName.replace(".zip", ".json")}`);
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

const update_view = async (dama_view_id, ctx) => {
  // update view meta
  const updateViewMetaSql = dedent(
    `
        UPDATE data_manager.views
          SET
            table_schema  = $1,
            table_name    = $2,
            data_table    = $3
          WHERE id = $4
      `
  );

  const data_table = pgFormat("%I.%I", "geo", `tl_2017_cousub_${dama_view_id}`);

  return ctx.call("dama_db.query", {
    text: updateViewMetaSql,
    values: ["geo", `tl_2017_cousub_${dama_view_id}`, data_table, dama_view_id],
  });

};

const mergeTables = async (ctx, fileNames, view_id) => {
  let sql = fileNames.map(file =>
    `SELECT geom, statefp, countyfp, geoid2 as geoid, name2 as name, namelsad FROM geo.${file} `
    ).join(` UNION ALL `);

  sql = `with t as (
                    ${sql}
                    )

        SELECT * INTO geo.tl_2017_cousub_${view_id} FROM t;`;

  return ctx.call("dama_db.query", {
    text: sql,
  });
};


export default async function publish(ctx: Context) {
  // throw new Error("publish TEST ERROR");

  const etlcontextid = await ctx.call(
    "dama_dispatcher.spawnDamaContext",
    { etl_context_id: null, pgEnv: null }
  );

  const {
    // @ts-ignore
    params: { etl_context_id = etlcontextid, src_id },
    meta: { pgEnv },
  } = ctx;

  if (!(etl_context_id)) {
    throw new Error("The etl_context_id parameter is required.");
  }

  try {
    // step 1
    const files = await getFiles(url);

    await files.reduce(async (acc, curr) => {
              await acc;
              return fetchFileList(curr).then(() => uploadFiles(curr, pgEnv));
            }, Promise.resolve());

    const {
      id: dama_view_id,
      table_schema: origTableSchema,
      table_name: origTableName,
    } = await create_view(etl_context_id, ctx);

    await mergeTables(ctx, files.map(f => f.replace(".zip", "")), dama_view_id);

    await update_view(dama_view_id, ctx);

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
