import { ReadStream } from "fs";
import round from "lodash/round";

// import { receiveDataset } from './utils'
import GeospatialDatasetIntegrator from "../../../../tasks/gis-data-integration/src/data_integrators/GeospatialDatasetIntegrator";

import EventTypes from "../EventTypes";

export default async function uploadGeospatialDataset(ctx) {
  const {
    params,
    meta: {
      // @ts-ignore
      filename,
      // @ts-ignore
      $multipart: uploadMetadata,
    },
  } = ctx;
  // https://moleculer.services/docs/0.14/moleculer-web.html#File-upload-aliases
  console.log('get to uploadMetadata')
  const fileStream = <ReadStream>params;
  const etlContextId = +uploadMetadata.etlContextId;
  const user_id = +uploadMetadata.user_id;
  const fileSizeBytes = +uploadMetadata.fileSizeBytes;
  const progressUpdateIntervalMs = Math.max(
    +uploadMetadata.progressUpdateIntervalMs,
    2500
  );

  if (!(etlContextId && user_id)) {
    throw new Error(
      "gis_dataset.uploadGeospatialDataset requires etlContextId, pgEnv, and user_id"
    );
  }
  try {
    const startEvent = {
      type: EventTypes.START_GIS_FILE_UPLOAD,
      payload: {
        ...uploadMetadata,
        etlContextId,
        user_id,
        fileSizeBytes,
        filename,
      },
      meta: {
        etl_context_id: etlContextId,
        user_id,
      },
    };
    await ctx.call("data_manager/events.dispatch", startEvent);

    const gdi = new GeospatialDatasetIntegrator();
    const receiveDatasetDoneData = gdi.receiveDataset(
      <string>filename,
      fileStream
    );

    let received100pct = false;

    if (fileSizeBytes) {
      // immediately invoked async function
      (async function dispatchProgressEvent() {
        if (received100pct) {
          return;
        }

        const progress = round((fileStream.bytesRead / fileSizeBytes) * 100);

        // CONSIDER: May want to clean these out of the store after upload complete.
        const progressEvent = {
          type: EventTypes.GIS_FILE_UPLOAD_PROGRESS,
          payload: { progress: `${progress}%` },
          meta: {
            etl_context_id: etlContextId,
            timestamp: new Date().toISOString(),
          },
        };

        await ctx.call("data_manager/events.dispatch", progressEvent);

        if (progress === 100) {
          received100pct = true;
        } else {
          setTimeout(dispatchProgressEvent, progressUpdateIntervalMs);
        }
      })();
    }

    fileStream.on("close", async () => {
      received100pct = true;

      const progressEvent = {
        type: EventTypes.GIS_FILE_RECEIVED,
        payload: { progress: "100%" },
        meta: {
          etl_context_id: etlContextId,
          timestamp: new Date().toISOString(),
        },
      };

      await ctx.call("data_manager/events.dispatch", progressEvent);

      const startedAnalysis = {
        type: EventTypes.START_GIS_FILE_UPLOAD_ANALYSIS,
        meta: {
          etl_context_id: etlContextId,
          timestamp: new Date().toISOString(),
        },
      };

      await ctx.call("data_manager/events.dispatch", startedAnalysis);
    });

    const { id, datasetMetadata } = await receiveDatasetDoneData;

    const finishEvent = {
      type: EventTypes.FINISH_GIS_FILE_UPLOAD,
      payload: {
        gis_upload_id: id,
        gis_dataset_metadata: datasetMetadata,
      },
      meta: {
        etl_context_id: etlContextId,
      },
    };

    await ctx.call("data_manager/events.dispatch", finishEvent);

    console.log(JSON.stringify({ finishEvent }, null, 4));

    return { id };
  } catch (err) {
    console.error(err);

    const errEvent = {
      type: EventTypes.GIS_FILE_UPLOAD_ERROR,
      payload: (err as any).message,
      meta: {
        etl_context_id: etlContextId,
      },
      error: true,
    };

    await ctx.call("data_manager/events.dispatch", errEvent);
  }
}
