import {Context} from "moleculer";
import {eal} from "./sqls";
import {err, fin, init, update_view} from "../utils/macros";

export default async function publish(ctx: Context) {
  let {
    // @ts-ignore
    params: {table_name, source_name, existing_source_id, view_dependencies, version,
      hlr_table, hlr_schema,
      nri_schema, nri_table
    },
  } = ctx;

  const {etl_context_id, dbConnection, sqlLog} = await init(ctx);

  const {source_id} =  existing_source_id ? existing_source_id : await ctx.call("dama/metadata.createNewDamaSource", {name: source_name, type: 'eal'});

  const {view_id} = await ctx.call("dama/metadata.createNewDamaView", {source_id, view_dependencies: JSON.parse(view_dependencies), version});

  try {
    // create schema
    const createSchema = `CREATE SCHEMA IF NOT EXISTS ${hlr_schema};`;
    sqlLog.push(createSchema);
    await ctx.call("dama_db.query", {
      text: createSchema
    });

    // create table
    const createTableSql = eal(table_name, view_id, hlr_schema, hlr_table, nri_schema, nri_table);
    sqlLog.push(createTableSql);
    await ctx.call("dama_db.query", {text: createTableSql});

    await dbConnection.query("COMMIT;");

    await update_view({table_schema: hlr_schema, table_name, view_id, dbConnection, sqlLog});

    return fin({etl_context_id, ctx, dbConnection, payload: {view_id, source_id}});
  } catch (e) {
    return err({e, etl_context_id, sqlLog, ctx, dbConnection});
  }
}
