import {Context} from "moleculer";
import {PoolClient, QueryConfig, QueryResult} from "pg";
import dedent from "dedent";
import pgFormat from "pg-format";
import EventTypes from "../constants/EventTypes";
import {tables} from "./tables";
import fs from "fs";
import https from "https";
import {execSync} from "child_process";
import {loadFiles} from "./upload";
import {err, fin, init, update_view} from "../utils/macros";


// mol $ call 'dama/data_source_integrator.testUploadAction' --table_name details --#pgEnv dama_dev_1

const fetchFileList = async (currTable) => {
  console.log("fetching...");

  if (!fs.existsSync("data/" + currTable)) {
    fs.mkdirSync("data/" + currTable);
  }

  const url = `https://hazards.fema.gov/nri/Content/StaticDocuments/DataDownload//NRI_Table_Counties/NRI_Table_Counties.zip`;
  const file_path = "data/" + currTable + "/NRI_Table_Counties.zip";

  // execSync(`rm -rf ${"data/" + currTable}`);
  execSync(`curl -o ${file_path} '${url}'`);
  execSync(`unzip -o ${file_path} -d ${"data/" + currTable}`, {encoding: "utf-8"});
  // execSync(`rm -rf ${"data/" + currTable}`);
  console.log("unzipped");
};


export default async function publish(ctx: Context) {
  let {
    // @ts-ignore
    params: {table_name}
  } = ctx;

  const {etl_context_id, dbConnection, source_id, view_id, sqlLog} = await init({ctx, type: 'nri'});

  try {
    // create schema
    const createSchema = `CREATE SCHEMA IF NOT EXISTS ${tables[table_name](view_id).schema};`;
    sqlLog.push(createSchema);
    await ctx.call("dama_db.query", {
      text: createSchema,
    });

    console.log("downloading", table_name);
    // download
    await fetchFileList(table_name);

    // upload
    await loadFiles(ctx, view_id);

    await dbConnection.query("COMMIT;");

    await update_view({table_schema: tables[table_name](view_id).schema, table_name, view_id, dbConnection, sqlLog});

    return fin({etl_context_id, ctx, dbConnection, payload: {view_id, source_id}});
  } catch (e) {
    return err({e, etl_context_id, sqlLog, ctx, dbConnection});
  }
}
