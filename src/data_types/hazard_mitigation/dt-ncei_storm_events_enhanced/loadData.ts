import { Context } from "moleculer";
import { PoolClient, QueryConfig, QueryResult } from "pg";
import {FSA} from "flux-standard-action";
import dedent from "dedent";
import pgFormat from "pg-format";
import EventTypes from "../constants/EventTypes";
import {postProcess} from "./postUploadProcessing";
import {tables} from "../dt-ncei_storm_events/utils/tables";
import {err, fin, init, update_view} from "../utils/macros";


export default async function publish(ctx: Context) {
  // throw new Error("publish TEST ERROR");

  let {
    // @ts-ignore
    params: { table_name, source_name, existing_source_id, view_dependencies, version,
      ncei_table, ncei_schema, tract_schema, tract_table, cousub_schema, cousub_table, ztc_schema, ztc_table},
  } = ctx;

  const {etl_context_id, dbConnection, sqlLog} = await init(ctx);

  const {source_id} =  existing_source_id ? existing_source_id : await ctx.call("dama/metadata.createNewDamaSource", {name: source_name, type: 'ncei_storm_events_enhanced'});

  const {view_id} = await ctx.call("dama/metadata.createNewDamaView", {source_id, view_dependencies: JSON.parse(view_dependencies), version});

  try {
    let res: QueryResult;

    // create table
    const createTableSql = `
                SELECT * INTO ${ncei_schema}.${table_name || tables.details.name}${view_id ? `_${view_id}` : ``}
                    FROM (SELECT * FROM ${ncei_schema}.${ncei_table}) t;
    `;
    sqlLog.push(createTableSql);
    res = await ctx.call("dama_db.query", {
      text: createTableSql,
    });

    await postProcess(ctx, `${table_name}${view_id ? `_${view_id}` : ``}`, tract_schema, tract_table, cousub_schema, cousub_table, ztc_schema, ztc_table);
    console.log("post upload process finished.");

    await update_view({table_schema: ncei_schema, table_name, view_id, dbConnection, sqlLog});

    return fin({etl_context_id, ctx, dbConnection, payload: {view_id, source_id}});
  } catch (e) {
    return err({e, etl_context_id, sqlLog, ctx, dbConnection});
  }
}
