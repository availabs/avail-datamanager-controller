import {Context} from "moleculer";
import {per_basis_swd, pad_zero_losses, adjusted_dollar, adjusted_dollar_pop} from "./sqls";
import {err, fin, init, update_view} from "../utils/macros";


export default async function publish(ctx: Context) {
  // throw new Error("publish TEST ERROR");

  let {
    // @ts-ignore
    params: {
      table_name,
      ncei_table, ncei_schema, nri_schema, nri_table
    },
  } = ctx;

  const {etl_context_id, dbConnection, source_id, view_id, sqlLog} = await init({ctx, type: 'per_basis'});

  try {

    // create schema
    const createSchema = `CREATE SCHEMA IF NOT EXISTS ${ncei_schema};`;
    sqlLog.push(createSchema);
    await ctx.call("dama_db.query", {
      text: createSchema
    });

    // create table
    sqlLog.push(per_basis_swd(table_name, view_id, ncei_schema, ncei_table));
    await ctx.call("dama_db.query", {
      text: per_basis_swd(table_name, view_id, ncei_schema, ncei_table)
    });

    // postprocessing

    sqlLog.push(pad_zero_losses(table_name, view_id, ncei_schema, ncei_table, nri_schema, nri_table));
    await ctx.call("dama_db.query", {
      text: pad_zero_losses(table_name, view_id, ncei_schema, ncei_table, nri_schema, nri_table)
    });

    sqlLog.push(adjusted_dollar(table_name, view_id, ncei_schema));
    await ctx.call("dama_db.query", {
      text: adjusted_dollar(table_name, view_id, ncei_schema)
    });

    sqlLog.push(adjusted_dollar_pop(table_name, view_id, ncei_schema));
    await ctx.call("dama_db.query", {
      text: adjusted_dollar_pop(table_name, view_id, ncei_schema)
    });

    await dbConnection.query("COMMIT;");

    // update view meta
    await update_view({table_schema: ncei_schema, table_name, view_id, dbConnection, sqlLog});

    return fin({etl_context_id, ctx, dbConnection, payload: {view_id, source_id}});
  } catch (e) {
    return err({e, etl_context_id, sqlLog, ctx, dbConnection});
  }
}
