import { Context } from "moleculer";
import { PoolClient, QueryConfig, QueryResult } from "pg";
import {FSA} from "flux-standard-action";
import dedent from "dedent";
import pgFormat from "pg-format";
import EventTypes from "../constants/EventTypes";
import {tables} from "./tables";
import fs from "fs";
import https from "https";
import {execSync} from "child_process";
import {loadFiles} from "./upload";
import {err, fin, init, update_view} from "../utils/macros";

const fetchFileList = async (currTable, from, to) => {
  console.log("fetching...");

  if(!fs.existsSync("data/" + currTable)){
    fs.mkdirSync("data/" + currTable);
  }

  let years  = [];
  let promises = []

  for ( let year = from; year <= to; year++){ years.push(year) }

    years.forEach(async (year) => {
      console.log('year', year)

      const url = `https://www.rma.usda.gov/-/media/RMA/Cause-Of-Loss/Summary-of-Business-with-Month-of-Loss/colsom_${year}.ashx?la=en`;
      const file_path = "data/" + currTable + "/" + `${year}.zip`;
      const command = `zcat ${file_path} > ${file_path.replace('.zip', '.txt')}`
      const file = fs.createWriteStream(file_path);

      promises.push(
        new Promise((resolve) => {
          https.get(url, response => {
            response.pipe(file);
            console.log('got: ', url);
            file.on('finish', f => {
              file.close();

              file.once('close', () => {
                file.removeAllListeners();
              });

              resolve(execSync(command, {encoding: 'utf-8'}));
            });

          })
        })
      );
    });

  return Promise.all(promises).then(() => console.log('unzipped'))


  // return years.reduce(async (acc, year) => {
  //   console.log('unzipping', year)
  //   await acc;
  //   console.log('unzipping 1', year)
  //   const file_path = "data/" + currTable + "/" + `${year}.zip`;
  //   const command = `zcat ${file_path} > ${file_path.replace('.zip', '.txt')}`
  //   execSync(command, {encoding: 'utf-8'});
  //
  // }, Promise.resolve());

}

export default async function publish(ctx: Context) {
  // throw new Error("publish TEST ERROR");

  let {
    // @ts-ignore
    params: { table_name, source_name, version, existing_source_id },
  } = ctx;

  const {etl_context_id, dbConnection, sqlLog} = await init(ctx);

  const {source_id} = existing_source_id ? existing_source_id :
    await ctx.call("dama/metadata.createNewDamaSource", {name: source_name, type: table_name});

  const {view_id} = await ctx.call("dama/metadata.createNewDamaView", {source_id, version});

  try {
    // create schema
    const createSchema = `CREATE SCHEMA IF NOT EXISTS open_fema_data;`;
    sqlLog.push(createSchema);
    await ctx.call("dama_db.query", {
      text: createSchema
    });

    console.log("downloading",table_name);
    await fetchFileList(table_name, 1989, 2022);
    await loadFiles(ctx, view_id, dbConnection);

    await dbConnection.query("COMMIT;");

    // update view meta
    await update_view({table_schema: tables[table_name](view_id).schema, table_name, view_id, dbConnection, sqlLog});

    return fin({etl_context_id, ctx, dbConnection, payload: {view_id, source_id}});
  } catch (e) {
    return err({e, etl_context_id, sqlLog, ctx, dbConnection});
  }
}
