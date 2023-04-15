import { inspect } from "util";

import EventTypes from "../EventTypes";

import GeospatialDatasetIntegrator from "../../../../tasks/gis-data-integration/src/data_integrators/GeospatialDatasetIntegrator";

import dama_events from "data_manager/events";
import logger from "data_manager/logger";

import { TaskEtlContext } from "data_manager/contexts";

import { createSource, createView } from "./actions.without-context";

export default async function publish(etl_context: TaskEtlContext) {
  const {
    initial_event: {
      payload: {
        userId,

        gisUploadId,
        layerName,
        tableDescriptor,

        source_values,
      },
    },
    meta: { pgEnv: pg_env, etl_context_id },
  } = etl_context;

  let {
    initial_event: {
      payload: { source_id },
    },
  } = etl_context;

  try {
    // create a source if necessary
    if (!source_id) {
      const damaSource = await createSource(etl_context, source_values);

      logger.debug(`created DamaSource\n${inspect(damaSource)}`);

      // @ts-ignore
      source_id = damaSource.source_id;
    }

    if (!source_id) {
      throw new Error("Source not created");
    }

    // create a view
    const damaView = await createView(etl_context, {
      source_id,
      user_id: userId,
    });

    logger.debug(`created view\n${inspect(damaView)}`);

    // -- Stage the dataset directly into final location --
    const gdi = new GeospatialDatasetIntegrator(gisUploadId);
    tableDescriptor.tableSchema = damaView.table_schema || "gis_datasets";
    tableDescriptor.tableName = damaView.table_name; // `etlctx_${etl_context_id}_${uniqId}`;
    await gdi.persistLayerTableDescriptor(tableDescriptor);

    try {
      await gdi.loadTable({ layerName, pgEnv: pg_env });
    } catch (err) {
      //
    }

    const {
      table_schema: tableSchema,
      table_name: tableName,
      source_id: damaSourceId,
      view_id: damaViewId,
    } = damaView;

    await dama_events.setEtlContextSourceId(
      damaSourceId,
      etl_context_id,
      pg_env
    );

    logger.info(`PUBLISHED: ${tableSchema}.${tableName}`);

    const finalEvent = {
      type: EventTypes.FINAL,
      payload: {
        damaSourceId,
        damaViewId,
      },
      meta: {
        etl_context_id,
        user_id: userId,
        timestamp: new Date().toISOString(),
      },
    };

    logger.debug(`final event: ${inspect(finalEvent)}`);

    await dama_events.dispatch(finalEvent, etl_context_id, pg_env);

    // await txn.commit();

    return finalEvent;
  } catch (err) {
    logger.error((<Error>err).message);
    logger.error((<Error>err).stack);

    const errEvent = {
      type: EventTypes.PUBLISH_ERROR,
      payload: {
        message: (<Error>err).message,
        stack: (<Error>err).stack,
      },
      meta: {
        etl_context_id,
        user_id: userId,
      },
    };

    // Back to the parentCtx
    await dama_events.dispatch(errEvent, etl_context_id, pg_env);

    try {
      // await txn.rollback();
    } catch (err2) {
      //
      console.log("transaction rollback error");
    }
    throw err;
  }
}
