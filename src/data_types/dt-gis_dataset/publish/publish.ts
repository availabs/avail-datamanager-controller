import EventTypes from "../EventTypes";

//import conformDamaSourceViewTableSchema from "./actions/conformDamaSourceViewTableSchema";
//import initializeDamaSourceMetadataUsingViews from "./actions/initializeDamaSourceMetadataUsingViews";

import GeospatialDatasetIntegrator from "../../../../tasks/gis-data-integration/src/data_integrators/GeospatialDatasetIntegrator";

import { createSource, createView } from './actions' 

export default async function publish(ctx) {
  let {
    // @ts-ignore
    params: {
      etlContextId, 
      userId, 

      gisUploadId,
      layerName,
      tableDescriptor,

      source_id,
      source_values,

      schemaName,
      tableName
    },
    meta: {
      pgEnv,
      etl_context_id
    }
  } = ctx;

  const txn = await ctx.call("dama_db.createTransaction");
  
  try {
    txn.begin()
    let damaSource = null
    // create a source if necessary
    if(!source_id) {
      damaSource = await createSource(ctx, source_values)
      source_id = damaSource.source_id
    }
    //console.log('created source',  damaSource , null, 4), source_id);
    if (!source_id) {
      throw new Error('Source not created')
    }
    
  
    // create a view
    let damaView = await createView(ctx, {source_id, user_id: userId})
    console.log('created view', JSON.stringify( damaView , null, 4));

    // -- Stage the dataset directly into final location --
    const gdi = new GeospatialDatasetIntegrator(gisUploadId);
    tableDescriptor.tableSchema = damaView.table_schema || "gis_datasets";
    tableDescriptor.tableName = damaView.table_name //`etlctx_${etl_context_id}_${uniqId}`;
    await gdi.persistLayerTableDescriptor(tableDescriptor);

    const migration_result = await gdi.loadTable({ layerName, pgEnv });
    //console.log('publish table', migration_result)

    //await publishStagedDataset(txnCtx);

    let initializeDamaSourceMetadataWarning: string | undefined;

    // try {
    //   await initializeDamaSourceMetadataUsingViews(ctx);
    // } catch (err) {
    //   console.error(err);
    //   initializeDamaSourceMetadataWarning = err.message;
    // }

    // ??? Should this go above initializeDamaSourceMetadata ???
    let conformDamaSourceViewTableSchemaWarning: string | undefined;

    // try {
    //   await conformDamaSourceViewTableSchema(txnCtx);
    // } catch (err) {
    //   console.error(err);
    //   conformDamaSourceViewTableSchemaWarning = err.message;
    // }

    const {
      table_schema: tableSchema,
      table_name: tableName,
      source_id: damaSourceId,
      view_id: damaViewId,
    } = damaView;

    console.log(`PUBLISHED: ${tableSchema}.${tableName}`);

    const finalEvent = {
      type: EventTypes.FINAL,
      payload: {
        damaSourceId,
        damaViewId,
        queryLog: txn.queryLog,
        initializeDamaSourceMetadataWarning,
        conformDamaSourceViewTableSchemaWarning,
      },
      meta: {
        etl_context_id: etlContextId,
        user_id: userId,
        timestamp: new Date().toISOString(),
      },
    };

    console.log(JSON.stringify({ finalEvent }, null, 4));

    // Back to the parentCtx
    await ctx.call("dama_dispatcher.dispatch", finalEvent);

    await txn.commit();

    return finalEvent;
  } catch (err) {
    console.error(err);

    const errEvent = {
      type: EventTypes.PUBLISH_ERROR,
      payload: {
        message: err.message,
        queryLog: txn.queryLog,
      },
      meta: {
        etl_context_id: etlContextId,
        user_id: userId,
        timestamp: new Date().toISOString(),
      },
    };

    // Back to the parentCtx
    await ctx.call("dama_dispatcher.dispatch", errEvent);

    try {
      await txn.rollback();
    } catch (err2) {
      //
      cosole.log('transaction rollback error')
    }
    throw err;
  }
}
