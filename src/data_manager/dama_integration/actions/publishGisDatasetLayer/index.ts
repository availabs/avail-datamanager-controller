import { Context } from "moleculer";

import _ from "lodash";

import GisDatasetIntegrationEventTypes from "../../constants/EventTypes";

import createTransactionContext from "./actions/createTransactionContext";
import createNewDamaSource from "./actions/createNewDamaSource";
import createNewDamaView from "./actions/createNewDamaView";
import publishStagedDataset from "./actions/publishStagedDataset";
import conformDamaSourceViewTableSchema from "./actions/conformDamaSourceViewTableSchema";

import initializeDamaSourceMetadataUsingViews from "./actions/initializeDamaSourceMetadataUsingViews";

import { DamaView } from "./index.d";

export default async function publish(ctx: Context) {
  const {
    // @ts-ignore
    params: { etl_context_id, user_id },
  } = ctx;

  const { txn, txnCtx } = await createTransactionContext(ctx);

  try {
    await txn.begin();

    await createNewDamaSource(txnCtx);
    await createNewDamaView(txnCtx);
    await publishStagedDataset(txnCtx);

    let initializeDamaSourceMetadataWarning: string | undefined;

    try {
      await initializeDamaSourceMetadataUsingViews(txnCtx);
    } catch (err) {
      console.error(err);
      initializeDamaSourceMetadataWarning = err.message;
    }

    // ??? Should this go above initializeDamaSourceMetadata ???
    let conformDamaSourceViewTableSchemaWarning: string | undefined;

    try {
      await conformDamaSourceViewTableSchema(txnCtx);
    } catch (err) {
      console.error(err);
      conformDamaSourceViewTableSchemaWarning = err.message;
    }

    const {
      table_schema: tableSchema,
      table_name: tableName,
      source_id: damaSourceId,
      view_id: damaViewId,
    } = <DamaView>txnCtx.params.newDamaView;

    console.log(`PUBLISHED: ${tableSchema}.${tableName}`);

    const finalEvent = {
      type: GisDatasetIntegrationEventTypes.FINAL,
      payload: {
        damaSourceId,
        damaViewId,
        queryLog: txn.queryLog,
        initializeDamaSourceMetadataWarning,
        conformDamaSourceViewTableSchemaWarning,
      },
      meta: {
        etl_context_id,
        user_id,
        timestamp: new Date().toISOString(),
      },
    };

    console.log(JSON.stringify({ finalEvent }, null, 4));

    // Back to the parentCtx
    await txnCtx.call("dama_dispatcher.dispatch", finalEvent);

    await txn.commit();

    return finalEvent;
  } catch (err) {
    console.error(err);

    const errEvent = {
      type: GisDatasetIntegrationEventTypes.PUBLISH_ERROR,
      payload: {
        message: err.message,
        queryLog: txn.queryLog,
      },
      meta: {
        etl_context_id,
        user_id,
        timestamp: new Date().toISOString(),
      },
    };

    // Back to the parentCtx
    await ctx.call("dama_dispatcher.dispatch", errEvent);

    try {
      await txn.rollback();
    } catch (err2) {
      //
    }

    throw err;
  }
}
