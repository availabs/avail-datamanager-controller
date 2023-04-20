// TODO: Implement support for re-running a MBTiles creation using config from DB.

import { rename as renameAsync } from "fs/promises";
import { join, basename } from "path";

import { Context } from "moleculer";

import _ from "lodash";
import tmp from "tmp";

import { Feature } from "geojson";

import etlDir from "../../../../constants/etlDir";

import mbtilesDir from "../../../../constants/mbtilesDir";

import { getTimestamp } from "../../../../data_utils/time";

import createMbtilesTask from "./tasks/createMbTiles";

export default async function createDamaGisDatasetViewMbtiles(ctx: Context) {
  const {
    // @ts-ignore
    params: { damaViewId },
  } = ctx;

  const etl_context_id = await ctx.call("data_manager/events.spawnEtlContext");

  const { path: etlWorkDir, cleanupCallback: eltWorkDirCleanup } =
    await new Promise((resolve, reject) =>
      tmp.dir({ tmpdir: etlDir }, (err, path, cleanupCallback) => {
        if (err) {
          return reject(err);
        }

        resolve({ path, cleanupCallback });
      })
    );

  const [damaViewNamePrefix, damaViewGlobalId] = await Promise.all([
    ctx.call("dama/metadata.getDamaViewNamePrefix", {
      damaViewId,
    }),

    ctx.call("dama/metadata.getDamaViewGlobalId", {
      damaViewId,
    }),

    ctx.call("dama/metadata.getDamaViewMapboxPaintStyle", {
      damaViewId,
    }),
  ]);

  const layerName = <string>damaViewNamePrefix;

  const now = new Date();
  const timestamp = getTimestamp(now);

  const tilesetName = `${damaViewGlobalId}_${timestamp}`;
  const mbtilesFileName = `${tilesetName}.mbtiles`;
  const mbtilesFilePath = join(etlWorkDir, mbtilesFileName);

  const initialEvent = {
    type: "createDamaGisDatasetViewMbtiles:INITIAL",
    payload: {
      damaViewId,
      timestamp,
    },
    meta: { etl_context_id },
  };

  await ctx.call("data_manager/events.dispatch", initialEvent);

  const featuresAsyncIterator = <AsyncGenerator<Feature>>(
    await ctx.call(
      "dama/gis.makeDamaGisDatasetViewGeoJsonFeatureAsyncIterator",
      ctx.params
    )
  );

  try {
    const { tippecanoeArgs, tippecanoeStdout, tippecanoeStderr } =
      await createMbtilesTask({
        layerName,
        mbtilesFilePath,
        featuresAsyncIterator,
        etlWorkDir,
      });

    const mbtilesBaseName = basename(mbtilesFilePath);
    const servedMbtilesPath = join(mbtilesDir, mbtilesBaseName);

    await renameAsync(mbtilesFilePath, servedMbtilesPath);

    const source_id = damaViewGlobalId;
    const source_layer_name = layerName;
    const source_type = "vector";

    const newRow = {
      view_id: damaViewId,
      tileset_timestamp: now,

      tileset_name: tilesetName,
      source_id,
      source_layer_name,
      source_type,
      tippecanoe_args: JSON.stringify(tippecanoeArgs),
    };

    const {
      rows: [{ mbtiles_id }],
    } = await ctx.call("dama/metadata.insertNewRow", {
      tableSchema: "_data_manager_admin",
      tableName: "dama_views_mbtiles_metadata",
      newRow,
    });

    const finalEvent = {
      type: "createDamaGisDatasetViewMbtiles:FINAL",
      payload: {
        view_id: damaViewId,
        mbtiles_id,
        tippecanoeStdout,
        tippecanoeStderr,
      },
      meta: { etl_context_id, timestamp: now },
    };

    await ctx.call("data_manager/events.dispatch", finalEvent);
  } catch (err) {
    const errorEvent = {
      type: "createDamaGisDatasetViewMbtiles:ERROR",
      payload: { message: err.message },
      error: true,
      meta: { etl_context_id },
    };

    await ctx.call("data_manager/events.dispatch", errorEvent);

    throw err;
  } finally {
    await eltWorkDirCleanup();
  }
}
