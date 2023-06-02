import EventTypes from "../EventTypes";

import dama_events from "data_manager/events";
import dama_db from "data_manager/dama_db";
import { getPgEnv } from "data_manager/contexts";
import logger from "data_manager/logger";

import { createViewMbtiles } from "./../mbtiles/mbtiles2";

import GeospatialDatasetIntegrator from "../../../../tasks/gis-data-integration/src/data_integrators/GeospatialDatasetIntegrator";

import { createSource, createView } from "./actions";

export default async function publish({
  etlContextId,
  userId,

  gisUploadId,
  layerName,
  tableDescriptor,

  source_id,
  source_values,

  schemaName,
  customViewAttributes,
  viewMetadata,
  viewDependency,

  isNewSourceCreate,
}) {
  logger.info(
    `inside publish first,
    etlContextId: ${etlContextId},
    userId: ${userId},
    gisUploadId: ${gisUploadId},
    layerName: ${layerName},
    tableDescriptor: ${tableDescriptor},
    source_id: ${source_id},
    source_values: ${source_values},
    schemaName: ${schemaName},
    customViewAttributes: ${JSON.stringify(customViewAttributes, null, 3)},
    viewMetadata: ${JSON.stringify(viewMetadata, null, 3)},
    viewDependency: ${viewDependency}
    isNewSourceCreate: ${isNewSourceCreate}`
  );

  const pgEnv = getPgEnv();

  let damaSource: any = null;

  if (!source_id) {
    logger.info("Reached here inside publish source create");
    damaSource = await createSource(source_values);
    logger.info(`New Source Created:  ${JSON.stringify(damaSource, null, 3)}`);
    source_id = damaSource?.source_id;
  }

  if (!source_id) {
    logger.error("Source not created");
    throw new Error("Source not created");
  }

  logger.info("\n\nAbout to Create the View");
  // create a view
  const damaView = await createView({
    source_id,
    user_id: userId,
    customViewAttributes,
    viewMetadata,
    viewDependency,
  });

  logger.info(`\n\nNew View created:  ${JSON.stringify(damaView, null, 3)}`);

  if (tableDescriptor && gisUploadId) {
    const gdi = new GeospatialDatasetIntegrator(gisUploadId);
    tableDescriptor.tableSchema = damaView.table_schema || "gis_datasets";
    tableDescriptor.tableName = damaView.table_name;

    await gdi.persistLayerTableDescriptor(tableDescriptor);

    try {
      // const migration_result = await gdi.loadTable({ layerName, pgEnv });
      logger.info(`inside the load table : layerName: ${layerName} and PgEnv: ${pgEnv}`);
      await gdi.loadTable({ layerName, pgEnv });
    } catch (err) {
      logger.error(`migration error -- \n ${JSON.stringify(err, null, 3)}`);
    }
    const {
      table_schema: tableSchema,
      table_name: tableName,
      source_id: damaSourceId,
      view_id: damaViewId,
    } = damaView;

    if (isNewSourceCreate) {
      logger.info("called inside setSourceMetadata");
      await dama_db.query({
        text: "CALL _data_manager_admin.initialize_dama_src_metadata_using_view( $1 )",
        values: [damaViewId],
      });
    }

    if (etlContextId && damaSourceId) {
      logger.info("Inside the setEtlContextSourceId Call");
      // dama_events.setEtlContextSourceId(etlContextId, damaSourceId);
    }

    // UN WANTED
    // ctx.params.damaViewId = damaViewId;
    // ctx.meta.etl_context_id = etlContextId;

    logger.info(
      `Outside the createViewMbtiles damaViewId: ${damaViewId}, damaViewId: ${damaViewId}, etlContextId: ${etlContextId}`
    );
    if (etlContextId && damaViewId && damaSourceId) {
      logger.info("Inside the createViewMbtiles Call");
      logger.info(`Inside the createViewMbtiles damaViewId: ${damaViewId}`);
      logger.info(
        `Inside the createViewMbtiles damaSourceId: ${damaSourceId}`
      );
      logger.info(
        `Inside the createViewMbtiles etlContextId: ${etlContextId}`
      );
      await createViewMbtiles(damaViewId, damaSourceId, etlContextId);
    }
  } else {
    const startEvent = {
      type: EventTypes.START_GIS_FILE_UPLOAD,
      payload: {
        etlContextId,
      },
      meta: {
        etl_context_id: etlContextId,
        user_id: userId,
        timestamp: new Date().toISOString(),
      },
    };
    logger.info(`Should I dispatch start event of the ACS ${JSON.stringify(startEvent, null, 3)}`);
    await dama_events.dispatch(startEvent, etlContextId);
  }

  // if ((!source_id && newDamaViewId)) {
  //   logger.debug(`newDamaViewId inside ::  ${newDamaViewId}`);
  //   logger.info("called inside setSourceMetadata");
  //   await dama_db.query({
  //     text: "CALL _data_manager_admin.initialize_dama_src_metadata_using_view( $1 )",
  //     values: [newDamaViewId],
  //   });
  // }

  const finalEvent = {
    type: EventTypes.FINAL,
    payload: {
      damaSourceId: (damaSource as any)?.source_id,
      damaViewId: damaView?.view_id,
    },
    meta: {
      user_id: userId,
      timestamp: new Date().toISOString(),
    },
  };

  logger.info(`About to dispatch final event ${JSON.stringify(finalEvent, null, 3)}`);
  await dama_events.dispatch(finalEvent, etlContextId);

  return finalEvent;
}
