import { Context } from "moleculer";
import {loadFiles, createSqls} from "./dt-ncei_storm_events/utils/upload";
import {tables} from "./dt-ncei_storm_events/utils/tables";
import {err, fin, init, update_view} from "./utils/macros";

export default async function publish(ctx: Context) {
  const {
    // @ts-ignore
    params: { table_name },
  } = ctx;

  const {etl_context_id, dbConnection, source_id, view_id, sqlLog} = await init({ctx, type: table_name});

  try {
    // create schema
    const createSchema = `CREATE SCHEMA IF NOT EXISTS ${tables[table_name].schema};`;
    sqlLog.push(createSchema);
    await ctx.call("dama_db.query", {
      text: createSchema,
    });

    // create table
    sqlLog.push(createSqls(table_name, view_id));
    await ctx.call("dama_db.query", {
      text: createSqls(table_name, view_id)
    });

    await loadFiles(table_name, view_id, ctx, "|");

    // update view meta
    await update_view({table_schema: tables[table_name].schema, table_name, view_id, dbConnection, sqlLog});

    return fin({etl_context_id, ctx, dbConnection, payload: {view_id, source_id}});
  } catch (e) {
    return err({e, etl_context_id, sqlLog, ctx, dbConnection});
  }
}
