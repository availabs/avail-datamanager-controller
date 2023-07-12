import {Context} from "moleculer";
import {tables} from "./tables";
import fs from "fs";
import {execSync} from "child_process";
import {loadFiles} from "./upload";
import {err, fin, init, update_view} from "../utils/macros";


// mol $ call 'dama/data_source_integrator.testUploadAction' --table_name details --#pgEnv dama_dev_1

const fetchFileList = async (currTable) => {
  console.log("fetching...");

  if (!fs.existsSync("tmp-etl/" + currTable)) {
    fs.mkdirSync("tmp-etl/" + currTable);
  }

  const url = `https://hazards.fema.gov/nri/Content/StaticDocuments/DataDownload//NRI_Table_CensusTracts/NRI_Table_CensusTracts.zip`;
  const file_path = "tmp-etl/" + currTable + "/NRI_Table_Tracts.zip";

  // execSync(`rm -rf ${"tmp-etl/" + currTable}`);
  execSync(`curl -o ${file_path} '${url}'`);
  execSync(`unzip -o ${file_path} -d ${"tmp-etl/" + currTable}`, {encoding: "utf-8"});
  // execSync(`rm -rf ${"tmp-etl/" + currTable}`);
  console.log("unzipped");
};


export default async function publish(ctx: Context) {
  let {
    // @ts-ignore
    params: {table_name}
  } = ctx;

  const {etl_context_id, dbConnection, source_id, view_id, sqlLog} = await init({ctx, type: 'nri_tracts'});

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
