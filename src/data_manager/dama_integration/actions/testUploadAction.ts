import { Context } from "moleculer";

import { PoolClient, QueryConfig, QueryResult } from "pg";

import {loadFiles, createSqls} from "./upload";

import {postProcess} from "./postUploadProcessing";

import EventTypes from "../constants/EventTypes";

export const ReadyToPublishPrerequisites = [
  EventTypes.QA_APPROVED,
  EventTypes.VIEW_METADATA_SUBMITTED,
];
// mol $ call 'dama/data_source_integrator.testUploadAction' --table_name details --#pgEnv dama_dev_1
export default async function publish(ctx: Context) {
  // throw new Error("publish TEST ERROR");

  const etlcontextid = await ctx.call(
    "dama_dispatcher.spawnDamaContext",
    { etl_context_id: null }
  );

  const {
    // @ts-ignore
    params: { etl_context_id = etlcontextid, table_name},
  } = ctx;
  //
  if (!(etl_context_id)) {
    throw new Error("The etl_context_id parameter is required.");
  }

  const dbConnection: PoolClient = await ctx.call("dama_db.getDbConnection");
  const sqlLog: any[] = [];
  const resLog: QueryResult[] = [];

  try {
    let res: QueryResult;

    sqlLog.push("BEGIN ;");
    res = await dbConnection.query("BEGIN ;");
    resLog.push(res);


    const createSchema = `CREATE SCHEMA IF NOT EXISTS severe_weather_new;`;
    sqlLog.push(createSchema);
    res = await ctx.call("dama_db.query", {
      text: createSchema
    });
    resLog.push(res);
    console.log("see this:", res.rows)

    sqlLog.push(createSqls[table_name]);
    res = await ctx.call("dama_db.query", {
      text: createSqls[table_name]
    });
    resLog.push(res);
    console.log("see this:", res.rows)


    await loadFiles(table_name, ctx);
    console.log("uploaded!");

    await postProcess(ctx);
    console.log("post upload process finished.");

    const testUploadSql = `SELECT count(1) FROM severe_weather_new.${table_name};`
    sqlLog.push(testUploadSql);
    res = await ctx.call("dama_db.query", {
      text: testUploadSql
    });
    resLog.push(res);
    console.log("testing upload", res.rows);

    // We need the data_manager.views id
    dbConnection.query("COMMIT;");
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

    await ctx.call("dama_dispatcher.dispatch", finalEvent);

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

    await ctx.call("dama_dispatcher.dispatch", errEvent);

    await dbConnection.query("ROLLBACK;");

    throw err;
  }
}
