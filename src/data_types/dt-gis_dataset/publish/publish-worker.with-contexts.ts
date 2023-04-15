import { inspect } from "util";

import EventTypes from "../EventTypes";

import GeospatialDatasetIntegrator from "../../../../tasks/gis-data-integration/src/data_integrators/GeospatialDatasetIntegrator";

import dama_events from "data_manager/events";
import logger from "data_manager/logger";
import {
  verifyIsInTaskEtlContext,
  getPgEnv,
  getEtlContextId,
} from "data_manager/contexts";

import { createSource, createView } from "./actions.with-context";

export type InitialEvent = {
  payload: {
    source_id: number;
    gisUploadId: string;
    layerName: string;
    tableDescriptor: any;
    source_values: any;
    userId: number;
  };
};

export default async function publish(initial_event: InitialEvent) {
  //  verify that all methods in the call tree will be able to get pg_env and etl_context_id
  //    from the DamaContext (data_manager/contexts)
  verifyIsInTaskEtlContext();

  const {
    payload: {
      userId,

      gisUploadId,
      layerName,
      tableDescriptor,

      source_values,
    },
  } = initial_event;

  let {
    payload: { source_id },
  } = initial_event;

  try {
    // create a source if necessary
    if (!source_id) {
      const damaSource = await createSource(source_values);

      logger.debug(`created DamaSource\n${inspect(damaSource)}`);

      // @ts-ignore
      source_id = damaSource.source_id;
    }

    // console.log('created source',  damaSource , null, 4), source_id);
    if (!source_id) {
      throw new Error("Source not created");
    }

    // create a view
    const damaView = await createView({
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
      await gdi.loadTable({ layerName, pgEnv: getPgEnv() });
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
      <number>getEtlContextId(),
      damaSourceId
    );

    logger.info(`PUBLISHED: ${tableSchema}.${tableName}`);

    const finalEvent = {
      type: EventTypes.FINAL,
      payload: {
        damaSourceId,
        damaViewId,
      },
      meta: {
        user_id: userId,
        timestamp: new Date().toISOString(),
      },
    };

    logger.debug(`final event: ${inspect(finalEvent)}`);

    await dama_events.dispatch(finalEvent);

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
        user_id: userId,
      },
    };

    // Back to the parentCtx
    await dama_events.dispatch(errEvent);

    try {
      // await txn.rollback();
    } catch (err2) {
      //
      logger.error("transaction rollback error");
      logger.error((<Error>err2).message);
      logger.error((<Error>err2).stack);
    }

    throw err;
  }
}
