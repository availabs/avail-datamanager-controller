import { Context } from "moleculer";

import { PoolClient, QueryConfig, QueryResult } from "pg";
import EventTypes from "../constants/EventTypes";
import {err, fin, init} from "../utils/macros";

// mol $ call 'dama/data_source_integrator.csvUploadAction' --table_name details --#pgEnv dama_dev_1


export default async function publish(ctx: Context) {
  // throw new Error("publish TEST ERROR");

  const {
    // @ts-ignore
    params: { type },
  } = ctx;

  const {etl_context_id, dbConnection, sqlLog} = await init({ctx, type: null, createSource: false});

  try {
    let resSources: QueryResult;
    let resViews: QueryResult;

    const getSrcs = `SELECT * FROM data_manager.sources WHERE type ='${type}';`;
    sqlLog.push(getSrcs);
    resSources = await ctx.call("dama_db.query", {
      text: getSrcs
    });

    const getViews = `SELECT * FROM data_manager.views WHERE source_id IN (${resSources.rows.map(src => src.source_id)});`;

    sqlLog.push(getViews);
    resViews = await ctx.call("dama_db.query", {
      text: getViews
    });


    return fin({etl_context_id, ctx, dbConnection, other: {sources: resSources.rows, views: resViews.rows}});
  } catch (e) {
    return err({e, etl_context_id, sqlLog, ctx, dbConnection});
  }
}
