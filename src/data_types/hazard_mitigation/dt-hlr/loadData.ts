import {Context} from "moleculer";
import {QueryResult} from "pg";
import {hlr} from "./sqls";
import {err, fin, init, update_view} from "../utils/macros";

export default async function publish(ctx: Context) {
  // throw new Error("publish TEST ERROR");

  let {
    params: {
      table_name,
      pb_table, pb_schema,
      nri_schema, nri_table,
      state_schema, state_table,
      county_schema, county_table,
      ncei_schema, ncei_table,
    }
  } = ctx;

  const {etl_context_id, dbConnection, source_id, view_id, sqlLog} = await init({ctx, type: 'hlr'});

  try {
    let res: QueryResult;

    // create schema
    const createSchema = `CREATE SCHEMA IF NOT EXISTS ${pb_schema};`;
    sqlLog.push(createSchema);
    res = await ctx.call("dama_db.query", {
      text: createSchema
    });


    // create table
    const createTableSql = hlr(table_name, view_id, state_schema, state_table, county_schema, county_table,
      ncei_schema, ncei_table, pb_schema, pb_table, nri_schema, nri_table);

    sqlLog.push(createTableSql);
    console.log('sql', createTableSql)
    await ctx.call("dama_db.query", {text: createTableSql});

    await dbConnection.query("COMMIT;");

    // update view meta
    await update_view({table_schema: ncei_schema, table_name, view_id, dbConnection, sqlLog});

    return fin({etl_context_id, ctx, dbConnection, payload: {
        view_id,
        source_id
      }});
  } catch (e) {
    return err({e, etl_context_id, sqlLog, ctx, dbConnection});
  }
}
