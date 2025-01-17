import EventTypes from "../constants/EventTypes";
import { PoolClient } from "pg";
import dedent from "dedent";
import pgFormat from "pg-format";

export const init = async ({
  ctx,
  type,
  createSource = true,
  metadata = {},
}) => {
  const {
    params: {
      source_name,
      existing_source_id,
      existing_view_id,
      version = 1,
      view_dependencies = "{}",
    },
  } = ctx;

  let source_id = parseInt(existing_source_id, 10);
  let view_id = parseInt(existing_view_id, 10);

  // begin transaction
  const sqlLog: any[] = [];
  const dbConnection: PoolClient = await ctx.call("dama_db.getDbConnection");
  await dbConnection.query("BEGIN ;");
  sqlLog.push("BEGIN ;");

  if (createSource) {
    if (isNaN(source_id)) {
      ({ source_id } = await ctx.call("dama/metadata.createNewDamaSource", {
        name: source_name,
        type,
      }));
    }

    if (isNaN(view_id)) {
      ({ view_id } = await ctx.call("dama/metadata.createNewDamaView", {
        source_id,
        view_dependencies: JSON.parse(view_dependencies),
        version,
        metadata,
      }));
    }
  }

  // get etl id
  const etl_context_id = await ctx.call("data_manager/events.spawnEtlContext", {
    source_id,
  });

  if (!etl_context_id) {
    throw new Error("Error producing etl.");
  }

  // init event
  const initialEvent = {
    type: EventTypes.INITIAL,
    meta: { etl_context_id },
  };

  await ctx.call("data_manager/events.dispatch", initialEvent);

  return { etl_context_id, dbConnection, source_id, view_id, sqlLog };
};

export const update_view = async ({
  table_schema,
  table_name,
  view_id,
  dbConnection,
}) => {
  const updateViewMetaSql = dedent(
    `
      UPDATE data_manager.views
      SET table_schema = $1,
          table_name   = $2,
          data_table   = $3
      WHERE view_id = $4
    `
  );

  const data_table = pgFormat(
    "%I.%I",
    table_schema,
    `${table_name}_${view_id}`
  );

  const q = {
    text: updateViewMetaSql,
    values: [table_schema, `${table_name}_${view_id}`, data_table, view_id],
  };

  await dbConnection.query(q);
};

export const fin = async ({
  etl_context_id,
  ctx,
  dbConnection,
  payload = {},
  other = {},
}) => {
  await dbConnection.query("COMMIT;");
  dbConnection.release();

  const finalEvent = {
    type: EventTypes.FINAL,
    ...other,
    payload,
    meta: {
      timestamp: new Date().toISOString(),
      etl_context_id: 500,
    },
  };

  console.log("reached here", finalEvent);

  // await ctx.call("data_manager/events.dispatch", finalEvent);

  return finalEvent;
};

export const err = async ({ e, etl_context_id, sqlLog, ctx, dbConnection }) => {
  await dbConnection.query("ROLLBACK;");
  dbConnection.release();

  console.error(e);

  const errEvent = {
    type: EventTypes.PUBLISH_ERROR,
    payload: {
      message: e.message,
      successfulcreateSchema: sqlLog,
      successfulcreateTable: sqlLog,
    },
    meta: {
      timestamp: new Date().toISOString(),
      etl_context_id,
    },
  };

  await ctx.call("data_manager/events.dispatch", errEvent);

  throw e;
};
