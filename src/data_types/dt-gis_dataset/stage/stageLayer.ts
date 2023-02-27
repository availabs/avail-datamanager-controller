import _ from "lodash";
import { v4 as uuid } from "uuid";



import GeospatialDatasetIntegrator from "../../../../tasks/gis-data-integration/src/data_integrators/GeospatialDatasetIntegrator";

import {serviceName} from "../index.service";
import EventTypes from "../EventTypes";

export default async function stageLayerData(ctx) {
  const {
    // @ts-ignore
    params: { etl_context_id, layerName },
    // @ts-ignore
    meta: { pgEnv },
  } = ctx;

  if (!etl_context_id) {
    throw new Error("etl_context_id parameter is required");
  }

  const events = await ctx.call("dama_dispatcher.queryDamaEvents", {
    etl_context_id,
  }); // Filter out the future events

  const finishedUploadEvent = events.find(
    ({ type }) => type === EventTypes.FINISH_GIS_FILE_UPLOAD
  );

  if (!finishedUploadEvent) {
    throw new Error(
      `FINISH_GIS_FILE_UPLOAD event not found for etl_context_id ${etl_context_id}.`
    );
  }

  const {
    // @ts-ignore
    payload: { gis_upload_id },
  } = finishedUploadEvent;

  const stageReqEvent = {
    type: `${serviceName}:STAGE_LAYER_DATA_REQUEST`,
    payload: { etl_context_id, layerName },
    meta: {
      etl_context_id,
    },
  };

  await ctx.call("data_manager/events.dispatch", stageReqEvent);

  const gdi = new GeospatialDatasetIntegrator(gis_upload_id);

  const uniqId = uuid().replace(/[^0-9A-Z]/gi, "");

  // NOTE: Changes the  the staged table's tableSchema and tableName
  const tableDescriptor = await gdi.getLayerTableDescriptor(layerName);
  tableDescriptor.tableSchema = "staged_gis_datasets";
  tableDescriptor.tableName = `etlctx_${etl_context_id}_${uniqId}`;
  await gdi.persistLayerTableDescriptor(tableDescriptor);

  const migration_result = await gdi.loadTable({ layerName, pgEnv });

  const loadedEvent = {
    type: `${serviceName}:LAYER_DATA_STAGED`,
    payload: migration_result,
    meta: {
      // @ts-ignore
      etl_context_id,
    },
  };

  await ctx.call("data_manager/events.dispatch", loadedEvent);

  const qaRequestEvent = {
    type: EventTypes.QA_REQUEST,
    payload: _.pick(loadedEvent, ["tableSchema", "tableName"]),
    meta: {
      etl_context_id,
      timestamp: new Date().toISOString(),
    },
  };

  await ctx.call("data_manager/events.dispatch", qaRequestEvent);

  return migration_result;
}
