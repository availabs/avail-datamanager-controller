import {execSync} from "child_process";
import fs from "fs";
import { PoolClient, QueryConfig, QueryResult } from "pg";
import { Context } from "moleculer";
import {FSA} from "flux-standard-action";
import dedent from "dedent";
import pgFormat from "pg-format";
import EventTypes from "../constants/EventTypes";
import {getPostgresConnectionString} from "../../../../data_manager/dama_db/postgres/PostgreSQL"
import {getFiles} from "./scrapper_fail";
import {loadFiles} from "./upload";

// eslint-disable-next-line @typescript-eslint/no-var-requires
const shapefileToGeojson = require("shapefile-to-geojson");

export const ReadyToPublishPrerequisites = [
  EventTypes.QA_APPROVED,
  EventTypes.VIEW_METADATA_SUBMITTED,
];

// mol $ call 'dama/data_source_integrator.testDownloadAction' --table_name details --#pgEnv dama_dev_1

const fetchFileList = async (fileName, url, table) => {
  console.log("fetching...", url);
  if(!fs.existsSync(`data/sba/`)){
    fs.mkdirSync(`data/sba/`);
  }
  const file_path = "data/" + `sba/` + fileName;

  execSync(`curl -o ${file_path} '${url}'`);

  // execSync(`rm -rf data/tl_2017_${table}/tmp_dir`);
};

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


export default async function publish(ctx: Context) {
  // throw new Error("publish TEST ERROR");

  const etlcontextid = await ctx.call(
    "data_manager/events.spawnEtlContext",
    { etl_context_id: null, pgEnv: null, table: null }
  );

  const initalEvent = {
    type: EventTypes.INITIAL
  }

  await ctx.call("data_manager/events.dispatch", initialEvent);

  const {
    // @ts-ignore
    params: { etl_context_id = etlcontextid, table, view_id },
    meta: { pgEnv },
  } = ctx;

  if (!(etl_context_id)) {
    throw new Error("The etl_context_id parameter is required.");
  }

  try {
    // step 1
    const data_cleaning_file_path = "src/data_manager/dama_integration/actions/sba/dataCleaning.py";

    const files = await getFiles();

    await files.reduce(async (acc, curr, i) => {
              await acc;
              const fileName = curr.split("/")[curr.split("/").length - 1]
              return fetchFileList(fileName, curr, table);
            }, Promise.resolve());

    execSync(`python ${data_cleaning_file_path}`, { encoding: 'utf-8' });

    await loadFiles(ctx, view_id, table);

    execSync(`rm -rf data/sba`);

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

    await ctx.call("data_manager/events.dispatch", finalEvent);

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

    await ctx.call("data_manager/events.dispatch", errEvent);

    throw err;
  }
}
