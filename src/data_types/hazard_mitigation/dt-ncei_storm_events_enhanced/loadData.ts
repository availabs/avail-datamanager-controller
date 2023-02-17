import { Context } from "moleculer";
import { PoolClient, QueryConfig, QueryResult } from "pg";
import {FSA} from "flux-standard-action";
import dedent from "dedent";
import pgFormat from "pg-format";
import EventTypes from "../constants/EventTypes";
import {postProcess} from "./postUploadProcessing";
import {tables} from "../dt-ncei_storm_events/utils/tables";

// mol $ call 'dama/data_source_integrator.testUploadAction' --table_name details --#pgEnv dama_dev_1

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

const create_view = async (etl_context_id, dbConnection, ctx, sqlLog) => {
  const events: FSA[] = await ctx.call("data_manager/events.queryEvents", {
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

  sqlLog.push(insertViewMetaSql);
  const res = await dbConnection.query(insertViewMetaSql);

  const {
    rows: [viewMetadata],
  } = res;
  return viewMetadata;

}

const update_view = async (ncei_schema, table_name, view_id, dbConnection, sqlLog, resLog) => {
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

  const data_table = pgFormat("%I.%I", ncei_schema, `${table_name}_${view_id}`);

  const q = {
    text: updateViewMetaSql,
    values: [ncei_schema, `${table_name}_${view_id}`, data_table, view_id],
  };

  sqlLog.push(q);
  const res = await dbConnection.query(q);
  resLog.push(res);
};

export default async function publish(ctx: Context) {
  // throw new Error("publish TEST ERROR");

  let {
    // @ts-ignore
    params: { etl_context_id, table_name, src_id, view_id, ncei_table, ncei_schema, tract_schema, tract_table, cousub_schema, cousub_table, ztc_schema, ztc_table},
  } = ctx;
  //
  if (!(etl_context_id)) {
    const etlcontextid = await ctx.call(
      "data_manager/events.spawnEtlContext",
      { etl_context_id: null }
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
  console.log('tables', table_name, src_id, ncei_schema, ncei_table, tract_schema, tract_table, cousub_schema, cousub_table, ztc_schema, ztc_table);
  // let view_id = 64
  try {
    let res: QueryResult;

    sqlLog.push("BEGIN ;");
    res = await dbConnection.query("BEGIN ;");
    resLog.push(res);
    // // create schema
    // const createSchema = `CREATE SCHEMA IF NOT EXISTS ${tables[table_name].schema};`;
    // sqlLog.push(createSchema);
    // res = await ctx.call("dama_db.query", {
    //   text: createSchema
    // });
    // resLog.push(res);
    // console.log("see this:", res.rows)

    // create table
    const createTableSql = `
                SELECT * INTO ${ncei_schema}.${table_name || tables.details.name}${view_id ? `_${view_id}` : ``}
                    FROM (SELECT * FROM ${ncei_schema}.${ncei_table}) t;
    `;
    sqlLog.push(createTableSql);
    res = await ctx.call("dama_db.query", {
      text: createTableSql,
    });
    resLog.push(res);
    console.log("see this:", res.rows)

    await postProcess(ctx, `${table_name}${view_id ? `_${view_id}` : ``}`, tract_schema, tract_table, cousub_schema, cousub_table, ztc_schema, ztc_table);
    console.log("post upload process finished.");


    // update view meta

    await update_view(ncei_schema, table_name, view_id, dbConnection, sqlLog, resLog);

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
