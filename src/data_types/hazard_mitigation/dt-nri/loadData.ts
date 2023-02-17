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

const update_view = async (table_name, view_id, dbConnection, sqlLog, resLog) => {
  const updateViewMetaSql = dedent(
    `
      UPDATE data_manager.views
      SET table_schema = $1,
          table_name   = $2,
          data_table   = $3
      WHERE view_id = $4
    `
  );

  const data_table = pgFormat("%I.%I", tables[table_name](view_id).schema, `${table_name}_${view_id}`);

  const q = {
    text: updateViewMetaSql,
    values: [tables[table_name](view_id).schema, `${table_name}_${view_id}`, data_table, view_id],
  };

  sqlLog.push(q);
  const res = await dbConnection.query(q);
  resLog.push(res);
};

export default async function publish(ctx: Context) {
  // throw new Error("publish TEST ERROR");

  let {
    // @ts-ignore
    params: {etl_context_id, table_name, view_id},
  } = ctx;
  //
  if (!(etl_context_id)) {
    const etlcontextid = await ctx.call(
      "data_manager/events.spawnEtlContext",
      {etl_context_id: null}
    );
    etl_context_id = etlcontextid;
    throw new Error("The etl_context_id parameter is required.");
  }

  const initialEvent = {
    type: EventTypes.INITIAL,
    meta: {etl_context_id}
  }

  await ctx.call("data_manager/events.dispatch", initialEvent);

  const dbConnection: PoolClient = await ctx.call("dama_db.getDbConnection");
  const sqlLog: any[] = [];
  const resLog: QueryResult[] = [];

  // let view_id = 13
  try {
    let res: QueryResult;

    sqlLog.push("BEGIN ;");
    res = await dbConnection.query("BEGIN ;");
    resLog.push(res);
    //
    // create schema
    const createSchema = `CREATE SCHEMA IF NOT EXISTS ${tables[table_name](view_id).schema};`;
    sqlLog.push(createSchema);
    res = await ctx.call("dama_db.query", {
      text: createSchema,
    });
    resLog.push(res);
    console.log("see this:", res.rows);

    console.log("downloading", table_name);
    await fetchFileList(table_name);
    await loadFiles(ctx, view_id);

    await dbConnection.query("COMMIT;");
    //
    //
    // // update view meta
    await update_view(table_name, view_id, dbConnection, sqlLog, resLog);
    //
    // await dbConnection.query("COMMIT;");
    //
    //
    // await loadFiles(table_name, view_id, ctx);
    //
    // console.log("uploaded!");

    // We need the data_manager.views id
    await dbConnection.query("COMMIT;");
    dbConnection.release();

    const finalEvent = {
      type: EventTypes.FINAL,
      payload: {
        data_manager_view_id: -1,
        createSchema: sqlLog,
        createTable: sqlLog,
        publishCmdResults: resLog,
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
        successfulcreateSchema: sqlLog,
        successfulcreateTable: sqlLog,
        successfulPublishCmdResults: resLog,
      },
      meta: {
        timestamp: new Date().toISOString(),
        etl_context_id
      },
    };

    await ctx.call("data_manager/events.dispatch", errEvent);

    await dbConnection.query("ROLLBACK;");

    throw err;
  }
}
