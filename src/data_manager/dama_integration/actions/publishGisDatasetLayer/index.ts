import { Context } from "moleculer";

import _ from "lodash";

import { FSA } from "flux-standard-action";

import GisDatasetIntegrationEventTypes from "../../constants/EventTypes";

import checkIfReadyToPublish from "./actions/checkIfReadyToPublish";
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

  try {
    if (!(etl_context_id && user_id)) {
      throw new Error(
        "The etl_context_id and user_id parameters are required."
      );
    }

    const events: FSA[] = await ctx.call("data_manager/events.queryEvents", {
      etl_context_id,
    });

    await checkIfReadyToPublish(ctx, events);

    const eventsByType = events.reduce((acc, damaEvent: FSA) => {
      const { type } = damaEvent;

      acc[type] = acc[type] || [];
      acc[type].push(damaEvent);

      return acc;
    }, {});

    // @ts-ignore
    const pub_ctx = ctx.copy();
    pub_ctx.params = {
      events,
      eventsByType,
      newDamaSource: null,
      newDamaView: null,
    };

    pub_ctx.meta = {
      ..._.cloneDeep(ctx.meta),
      etl_context_id,
    };

    await createNewDamaSource(pub_ctx);
    await createNewDamaView(pub_ctx);
    await publishStagedDataset(pub_ctx);

    let initializeDamaSourceMetadataWarning: string | undefined;

    try {
      await initializeDamaSourceMetadataUsingViews(pub_ctx);
    } catch (err) {
      console.error(err);
      initializeDamaSourceMetadataWarning = err.message;
    }

    // ??? Should this go above initializeDamaSourceMetadata ???
    let conformDamaSourceViewTableSchemaWarning: string | undefined;

    try {
      await conformDamaSourceViewTableSchema(pub_ctx);
    } catch (err) {
      console.error(err);
      conformDamaSourceViewTableSchemaWarning = err.message;
    }

    const {
      table_schema: tableSchema,
      table_name: tableName,
      source_id: damaSourceId,
      view_id: damaViewId,
    } = <DamaView>pub_ctx.params.newDamaView;

    await pub_ctx.call("data_manager/events.setEtlContextSourceId", {
      etl_context_id,
      source_id: damaSourceId,
    });

    console.log(`PUBLISHED: ${tableSchema}.${tableName}`);

    const finalEvent = {
      type: GisDatasetIntegrationEventTypes.FINAL,
      payload: {
        damaSourceId,
        damaViewId,
        initializeDamaSourceMetadataWarning,
        conformDamaSourceViewTableSchemaWarning,
      },
      meta: {
        etl_context_id,
        user_id,
        timestamp: new Date().toISOString(),
      },
    };

    // console.log(JSON.stringify({ finalEvent }, null, 4));

    // Back to the parentCtx
    await ctx.call("data_manager/events.dispatch", finalEvent);

    return finalEvent;
  } catch (err) {
    console.error(err);

    const errEvent = {
      type: GisDatasetIntegrationEventTypes.PUBLISH_ERROR,
      payload: {
        message: err.message,
      },
      meta: {
        etl_context_id,
        user_id,
        timestamp: new Date().toISOString(),
      },
    };

    // Back to the parentCtx
    await ctx.call("data_manager/events.dispatch", errEvent);

    throw err;
  }
}
