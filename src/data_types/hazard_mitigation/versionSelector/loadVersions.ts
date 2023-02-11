import { Context } from "moleculer";

import { PoolClient, QueryConfig, QueryResult } from "pg";
import EventTypes from "../constants/EventTypes";

export const ReadyToPublishPrerequisites = [
  EventTypes.QA_APPROVED,
  EventTypes.VIEW_METADATA_SUBMITTED,
];
// mol $ call 'dama/data_source_integrator.csvUploadAction' --table_name details --#pgEnv dama_dev_1


export default async function publish(ctx: Context) {
  // throw new Error("publish TEST ERROR");

  const etlcontextid = await ctx.call(
    "dama_dispatcher.spawnDamaContext",
    { etl_context_id: null }
  );

  const {
    // @ts-ignore
    params: { etl_context_id = etlcontextid, type},
  } = ctx;
  //
  if (!(etl_context_id)) {
    throw new Error("The etl_context_id parameter is required.");
  }

  const dbConnection: PoolClient = await ctx.call("dama_db.getDbConnection");
  const sqlLog: any[] = [];
  const resLog: QueryResult[] = [];

  try {
    let resSources: QueryResult;
    let resViews: QueryResult;

    const getSrcs = `SELECT * FROM data_manager.sources WHERE type ='${type}';`;
    sqlLog.push(getSrcs);
    resSources = await ctx.call("dama_db.query", {
      text: getSrcs
    });
    resLog.push(resSources.rows);
    console.log("see this:", resSources.rows);

    const getViews = `SELECT * FROM data_manager.views WHERE source_id IN (${resSources.rows.map(src => src.source_id)});`;
    console.log('??', getSrcs)
    console.log('??', getViews)
    sqlLog.push(getViews);
    resViews = await ctx.call("dama_db.query", {
      text: getViews
    });
    resLog.push(resViews.rows);
    console.log("see this:", resViews.rows);


    await dbConnection.query("COMMIT;");
    dbConnection.release();

    const finalEvent = {
      type: EventTypes.FINAL,
      sources: resSources.rows,
      views: resViews.rows,
      payload: {
        data_manager_view_id: -1,
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
