import EventTypes from "../EventTypes";

//import conformDamaSourceViewTableSchema from "./actions/conformDamaSourceViewTableSchema";
//import initializeDamaSourceMetadataUsingViews from "./actions/initializeDamaSourceMetadataUsingViews";

import GeospatialDatasetIntegrator from "../../../../tasks/gis-data-integration/src/data_integrators/GeospatialDatasetIntegrator";

import { createSource, createView } from './actions' 

export default async function publish(ctx) {
  // params come from post data
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

  //const txn = await ctx.call("dama_db.createTransaction");
  
  try {
    // txn.begin()
    let damaSource = null
    let setSourceMetadata = false
    // create a source if necessary
    if(!source_id) {
      setSourceMetadata = true
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

    let loadTableWarning

    try {
      const migration_result = await gdi.loadTable({ layerName, pgEnv });
    } catch( err ) {

    }

    const {
      table_schema: tableSchema,
      table_name: tableName,
      source_id: damaSourceId,
      view_id: damaViewId,
    } = damaView;

    if(setSourceMetadata) {
      await ctx.call("dama_db.query", {
        text: `CALL _data_manager_admin.initialize_dama_src_metadata_using_view( $1 )`,
        values: [damaViewId],
      });
    }

    await ctx.call("data_manager/events.setEtlContextSourceId", {
      etl_context_id,
      source_id: damaSourceId,
    });

    console.log(`PUBLISHED: ${tableSchema}.${tableName}`);

    ctx.params.damaViewId = damaViewId
    ctx.meta.etl_context_id = etlContextId

    await ctx.call('gis-dataset.createViewMbtiles', {
      damaViewId,
      damaSourceId
    })

    // console.log('mbtilesData', JSON.stringify(mbtilesData,null,3))

    const finalEvent = {
      type: EventTypes.FINAL,
      payload: {
        damaSourceId,
        damaViewId
      },
      meta: {
        etl_context_id: etlContextId,
        user_id: userId,
        timestamp: new Date().toISOString(),
      },
    };

    console.log('final event')
    //console.log(JSON.stringify({ finalEvent }, null, 4));

    await ctx.call("data_manager/events.dispatch", finalEvent);

    //await txn.commit();

    return finalEvent;
  } catch (err) {
    console.error(err);

    const errEvent = {
      type: EventTypes.PUBLISH_ERROR,
      payload: {
        message: err.message
      },
      meta: {
        etl_context_id: etlContextId,
        user_id: userId,
        timestamp: new Date().toISOString(),
      },
    };

    // Back to the parentCtx
    await ctx.call("data_manager/events.dispatch", errEvent);

    try {
      //await txn.rollback();
    } catch (err2) {
      //
      cosole.log('transaction rollback error')
    }
    throw err;
  }
}
