import {Context} from "moleculer";
import {QueryResult} from "pg";
import {ofd} from "./sqls";
import {err, fin, init, update_view} from "../utils/macros";

export default async function publish(ctx: Context) {
  // throw new Error("publish TEST ERROR");

  let {
    params: {
      table_name, ofd_schema,
      pafpd_table, ihp_table, dds_table, sba_table,
      nfip_table, usda_table, hmgp_table
    }
  } = ctx;

  const {etl_context_id, dbConnection, source_id, view_id, sqlLog} = await init({ctx, type: 'disaster_loss_summary'});

  try {
    let res: QueryResult;

    // create schema
    const createSchema = `CREATE SCHEMA IF NOT EXISTS ${ofd_schema};`;
    sqlLog.push(createSchema);
    res = await ctx.call("dama_db.query", {
      text: createSchema
    });


    // create table
    const createTableSql = ofd({
      table_name, ofd_schema, view_id,
      pafpd_table, ihp_table, dds_table, sba_table,
      nfip_table, usda_table, hmgp_table
    });

    sqlLog.push(createTableSql);
    console.log('sql', createTableSql)
    await ctx.call("dama_db.query", {text: createTableSql});

    await dbConnection.query("COMMIT;");

    // update view meta
    await update_view({table_schema: ofd_schema, table_name, view_id, dbConnection, sqlLog});

    return fin({etl_context_id, ctx, dbConnection, payload: {
        view_id,
        source_id
      }});
  } catch (e) {
    return err({e, etl_context_id, sqlLog, ctx, dbConnection});
  }
}
