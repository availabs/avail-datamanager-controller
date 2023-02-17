import { Context } from "moleculer";

import { PoolClient, QueryConfig, QueryResult } from "pg";

import {loadFiles, createSqls} from "./dt-ncei_storm_events/utils/upload";
import {tables} from "./dt-ncei_storm_events/utils/tables";

import EventTypes from "./constants/EventTypes";
import {FSA} from "flux-standard-action";
import dedent from "dedent";
import pgFormat from "pg-format";

export const ReadyToPublishPrerequisites = [
  EventTypes.QA_APPROVED,
  EventTypes.VIEW_METADATA_SUBMITTED,
];
// mol $ call 'dama/data_source_integrator.csvUploadAction' --table_name details --#pgEnv dama_dev_1

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
export default async function publish(ctx: Context) {
  // throw new Error("publish TEST ERROR");

  const etlcontextid = await ctx.call(
    "data_manager/events.spawnEtlContext",
    { etl_context_id: null }
  );

  const initialEvent = {
    type: EventTypes.INITIAL,
    meta: {etl_context_id: etlcontextid}
  }

  await ctx.call("data_manager/events.dispatch", initialEvent);

  const {
    // @ts-ignore
    params: { etl_context_id = etlcontextid, table_name, src_id, view_id},
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

    // create schema
    const createSchema = `CREATE SCHEMA IF NOT EXISTS ${tables[table_name].schema};`;
    // console.log('create', createSchema)
    sqlLog.push(createSchema);
    res = await ctx.call("dama_db.query", {
      text: createSchema,
    });
    resLog.push(res);
    console.log("see this:", res.rows);

    // create table
    sqlLog.push(createSqls(table_name, view_id));
    // console.log('create table', createSqls(table_name, view_id));
    res = await ctx.call("dama_db.query", {
      text: createSqls(table_name, view_id)
    });
    resLog.push(res);
    console.log("see this:", res.rows)



    await loadFiles(table_name, view_id, ctx, "|");
    console.log("uploaded!");

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

    const data_table = pgFormat("%I.%I", tables[table_name].schema, `${table_name}_${view_id}`);

    const q = {
      text: updateViewMetaSql,
      values: [tables[table_name].schema, `${table_name}_${view_id}`, data_table, view_id],
    };

    sqlLog.push(q);
    res = await dbConnection.query(q);
    resLog.push(res);



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
