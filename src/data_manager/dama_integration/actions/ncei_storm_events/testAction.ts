import { Context } from "moleculer";

import { PoolClient, QueryConfig, QueryResult } from "pg";
import pgFormat from "pg-format";
import dedent from "dedent";
import _ from "lodash";

import { FSA } from "flux-standard-action";

import EventTypes from "../../constants/EventTypes";

export const ReadyToPublishPrerequisites = [
  EventTypes.QA_APPROVED,
  EventTypes.VIEW_METADATA_SUBMITTED,
];

export default async function publish(ctx: Context) {
  // throw new Error("publish TEST ERROR");

  const etlcontextid = await ctx.call(
    "dama_dispatcher.spawnDamaContext",
    { etl_context_id: null }
  );

  const {
    // @ts-ignore
    params: { etl_context_id = etlcontextid},
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

    const publishSql = "SELECT count(1) from _data_manager_admin.event_store_prototype;";

    sqlLog.push(publishSql);

    res = await ctx.call("dama_db.query", {
      text: publishSql
    });
    resLog.push(res);
    console.log('see this:', res.rows)
    // We need the data_manager.views id
    dbConnection.query("COMMIT;");
    dbConnection.release();

    const finalEvent = {
      type: EventTypes.FINAL,
      payload: {
        data_manager_view_id: -1,
        publishSql: sqlLog,
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
        successfulPublishSql: sqlLog,
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
