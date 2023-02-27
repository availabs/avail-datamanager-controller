import { Context } from "moleculer";
import { PoolClient, QueryConfig, QueryResult } from "pg";
import {FSA} from "flux-standard-action";
import dedent from "dedent";
import pgFormat from "pg-format";
import EventTypes from "../constants/EventTypes";
import {loadFiles, createSqls} from "./utils/upload";
import {tables} from "./utils/tables";
import fs from "fs";
import https from "https";
import {execSync} from "child_process";
import {getFiles} from "./utils/scrapper";
import {err, fin, init, update_view} from "../utils/macros";

// mol $ call 'dama/data_source_integrator.testUploadAction' --table_name details --#pgEnv dama_dev_1

const fetchFileList = (fileName, currTable) => {
  const url = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
  console.log("fetching...", url + fileName)

  if(!fs.existsSync("data")){
    fs.mkdirSync("data")
  }

  if(!fs.existsSync("data/" + currTable)){
    fs.mkdirSync("data/" + currTable)
  }

  const file_path = "data/" + currTable + "/" + fileName;
  const file = fs.createWriteStream(file_path);

  return new Promise((resolve, reject) => {
    https.get(url + fileName, response => {
      response.pipe(file);
      console.log('got: ', url + fileName)
      file.on('finish', f => {
        file.close();
        file.once('close', () => {
          file.removeAllListeners();
        });

        resolve(execSync(`gunzip -f ${file_path}`, { encoding: 'utf-8' }));
      })
    })
  })
}

export default async function publish(ctx: Context) {
  // throw new Error("publish TEST ERROR");

  let {
    // @ts-ignore
    params: { table_name, source_name, existing_source_id, version },
  } = ctx;

  const {etl_context_id, dbConnection, source_id, view_id, sqlLog} = await init({ctx, type: 'ncei_storm_events'});

  try {
    let res: QueryResult;
    const data_cleaning_file_path = "src/data_types/hazard_mitigation/dt-ncei_storm_events/utils/dataCleaning.py";

    // download step 1
    const files = await getFiles();
    console.log("downloading",table_name);
    await [table_name]
      .reduce(async (accTable, currTable) => {
        await accTable;

        return files[currTable]
          .reduce(async (acc, curr) => {
            await acc;
            return fetchFileList(curr, currTable)
          }, Promise.resolve())
      }, Promise.resolve())

    // download step 2
    execSync(`python ${data_cleaning_file_path} ${table_name}`, { encoding: 'utf-8' });

    // create schema
    const createSchema = `CREATE SCHEMA IF NOT EXISTS ${tables[table_name].schema};`;
    sqlLog.push(createSchema);
    res = await ctx.call("dama_db.query", {
      text: createSchema
    });

    // create table
    sqlLog.push(createSqls(table_name, view_id));
    await ctx.call("dama_db.query", {
      text: createSqls(table_name, view_id)
    });

    await dbConnection.query("COMMIT;");

    await update_view({table_schema: tables[table_name].schema, table_name, view_id, dbConnection, sqlLog});

    await dbConnection.query("COMMIT;");

    await loadFiles(table_name, view_id, ctx);

    return fin({etl_context_id, ctx, dbConnection, payload: {view_id, source_id}});
  } catch (e) {
    return err({e, etl_context_id, sqlLog, ctx, dbConnection});
  }
}
