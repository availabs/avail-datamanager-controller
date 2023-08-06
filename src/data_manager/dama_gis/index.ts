import { rename as renameAsync } from "fs/promises";
import { join, basename } from "path";

import pgFormat from "pg-format";
import dedent from "dedent";
import _ from "lodash";
import tmp from "tmp";

import { NodePgQueryResult } from "data_manager/dama_db/postgres/PostgreSQL";

import asyncGeneratorToNdjsonStream from "data_utils/streaming/asyncGeneratorToNdjsonStream";

import dama_db from "data_manager/dama_db";
import dama_events from "data_manager/events";
import dama_meta from "data_manager/meta";
import { runInDamaContext, getPgEnv } from "data_manager/contexts";

import mbtilesDir from "constants/mbtilesDir";
import etlDir from "constants/etlDir";
import { getTimestamp } from "data_utils/time";

import createMbtilesTask from "./tasks/createMbTiles";

import DamaContextAttachedResource from "data_manager/contexts";
import { DamaViewID } from "data_manager/meta/domain";

class DamaGis extends DamaContextAttachedResource {
  async getDamaGisDatasetViewTableSchemaSummary(dama_view_id: DamaViewID) {
    const damaViewPropsColsQ = dedent(`
      SELECT
          column_name,
          is_geometry_col
        FROM _data_manager_admin.dama_table_column_types
        WHERE ( view_id = $1 )
    `);

    const { rows } = await dama_db.query({
      text: damaViewPropsColsQ,
      values: [dama_view_id],
    });

    if (rows.length === 0) {
      throw new Error(`Invalid DamaViewId: ${dama_view_id}`);
    }

    const nonGeometryColumns = rows
      .filter(({ is_geometry_col }) => !is_geometry_col)
      .map(({ column_name }) => column_name);

    const { column_name: geometryColumn = null } =
      rows.find(({ is_geometry_col }) => is_geometry_col) || {};

    const damaViewIntIdQ = dedent(`
          SELECT
              table_schema,
              table_name,
              primary_key_summary,
              int_id_column_name
            FROM _data_manager_admin.dama_views_int_ids
            WHERE ( view_id = $1 )
        `);

    const damaViewIntIdRes: NodePgQueryResult = await dama_db.query({
      text: damaViewIntIdQ,
      values: [dama_view_id],
    });

    if (!damaViewIntIdRes.rows.length) {
      throw new Error(
        `Unable to get primary key metadata for DamaView ${dama_view_id}`
      );
    }

    const {
      rows: [
        {
          table_schema: tableSchema,
          table_name: tableName,
          primary_key_summary,
          int_id_column_name: intIdColName,
        },
      ],
    } = damaViewIntIdRes;

    const primaryKeyCols = primary_key_summary.map(
      ({ column_name }) => column_name
    );

    return {
      tableSchema,
      tableName,
      primaryKeyCols,
      intIdColName,
      nonGeometryColumns,
      geometryColumn,
    };
  }

  async generateGisDatasetViewGeoJsonSqlQuery(
    dama_view_id: DamaViewID,
    config = {} as { properties?: string | string[] }
  ) {
    let { properties } = config;

    if (properties && properties !== "*" && !Array.isArray(properties)) {
      properties = [properties];
    }

    const {
      tableSchema,
      tableName,
      intIdColName,
      nonGeometryColumns,
      geometryColumn,
    } = await this.getDamaGisDatasetViewTableSchemaSummary(dama_view_id);

    if (!geometryColumn) {
      throw new Error(
        `DamaView's ${dama_view_id} does not appear to be a GIS dataset.`
      );
    }

    let props: string[] = [];

    if (properties === "*") {
      props = nonGeometryColumns;
    }

    if (Array.isArray(properties)) {
      const invalidProps = _.difference(properties, nonGeometryColumns);

      if (invalidProps.length) {
        throw new Error(
          `The following requested properties are not in the DamaView's data table: ${invalidProps}`
        );
      }

      props = _.uniq(properties);
    }

    const featureIdBuildObj = {
      text: intIdColName ? "'id', %I," : "",
      values: intIdColName ? [intIdColName] : [],
    };

    const propsBuildObj = props.reduce(
      (acc, prop) => {
        acc.placeholders.push("%L");
        acc.placeholders.push("%I");

        acc.values.push(prop, prop);

        return acc;
      },
      { placeholders: <string[]>[], values: <string[]>[] }
    );

    const selectColsClause = _.uniq([intIdColName, geometryColumn, ...props])
      .filter(Boolean)
      .reduce(
        (acc, col) => {
          acc.placeholders.push("%I");
          acc.values.push(col);

          return acc;
        },
        { placeholders: [], values: [] }
      );

    const sql = pgFormat(
      `
        SELECT
            jsonb_build_object(
              'type',       'Feature',
              ${featureIdBuildObj.text}
              'properties', jsonb_build_object(${propsBuildObj.placeholders}),
              'geometry',   ST_AsGeoJSON(%I)::JSON
            ) AS feature
          FROM (
            SELECT ${selectColsClause.placeholders}
              FROM %I.%I
          ) row;
      `,
      ...featureIdBuildObj.values,
      ...propsBuildObj.values,
      geometryColumn,
      ...selectColsClause.values,
      tableSchema,
      tableName
    );

    return sql;
  }
  // List of the GIS Dataset uploads in the etl-work-dir directory.
  async *makeDamaGisDatasetViewGeoJsonFeatureAsyncIterator(
    dama_view_id: DamaViewID,
    config = {} as { properties?: string | string[]; row_count?: number }
  ) {
    const sql = await this.generateGisDatasetViewGeoJsonSqlQuery(
      dama_view_id,
      config
    );

    const iter = <AsyncGenerator<any>>dama_db.makeIterator(sql, config);

    for await (const { feature } of iter) {
      yield feature;
    }
  }

  makeDamaGisDatasetViewGeoJsonlStream(
    dama_view_id: DamaViewID,
    config = {} as { properties?: string | string[]; row_count?: number }
  ) {
    const iter = this.makeDamaGisDatasetViewGeoJsonFeatureAsyncIterator(
      dama_view_id,
      config
    );

    return asyncGeneratorToNdjsonStream(iter);
  }

  // NOTE: Must be called from within a DamaContext.
  async createGisDatasetViewMbtiles(dama_view_id: DamaViewID) {
    const { path: etlWorkDir, cleanupCallback: eltWorkDirCleanup } =
      await new Promise<{ path: string; cleanupCallback: () => void }>(
        (resolve, reject) =>
          tmp.dir({ tmpdir: etlDir }, (err, path, cleanupCallback) => {
            if (err) {
              return reject(err);
            }

            resolve({ path, cleanupCallback });
          })
      );

    const [damaViewNamePrefix, damaViewGlobalId] = await Promise.all([
      dama_meta.getDamaViewNamePrefix(dama_view_id),
      dama_meta.getDamaViewGlobalId(dama_view_id),
      dama_meta.getDamaViewMapboxPaintStyle(dama_view_id),
    ]);

    const layerName = <string>damaViewNamePrefix;

    const now = new Date();
    const timestamp = getTimestamp(now);

    const tilesetName = `${damaViewGlobalId}_${timestamp}`;
    const mbtilesFileName = `${tilesetName}.mbtiles`;
    const mbtilesFilePath = join(etlWorkDir, mbtilesFileName);

    try {
      const featuresAsyncIterator =
        this.makeDamaGisDatasetViewGeoJsonFeatureAsyncIterator(dama_view_id);

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

      return {
        view_id: dama_view_id,
        tileset_timestamp: now,

        tileset_name: tilesetName,
        source_id,
        source_layer_name,
        source_type,
        tippecanoe_args: tippecanoeArgs,
        tippecanoeStdout,
        tippecanoeStderr,
      };
    } finally {
      eltWorkDirCleanup();
    }
  }

  async createDamaGisDatasetViewMbtiles(dama_view_id: DamaViewID) {
    const etl_context_id = await dama_events.spawnEtlContext();

    const ctx = {
      meta: { pgEnv: getPgEnv(), etl_context_id },
    };

    await runInDamaContext(ctx, async () => {
      const now = new Date();
      const timestamp = getTimestamp(now);

      const initialEvent = {
        type: "createDamaGisDatasetViewMbtiles:INITIAL",
        payload: {
          damaViewId: dama_view_id,
          timestamp,
        },
        meta: { etl_context_id },
      };

      await dama_events.dispatch(initialEvent);

      try {
        const {
          tileset_timestamp,
          tileset_name,
          source_id,
          source_layer_name,
          source_type,
          tippecanoe_args,
          tippecanoeStdout,
          tippecanoeStderr,
        } = await this.createGisDatasetViewMbtiles(dama_view_id);

        const newRow = {
          view_id: dama_view_id,
          tileset_timestamp,

          tileset_name,
          source_id,
          source_layer_name,
          source_type,
          tippecanoe_args: JSON.stringify(tippecanoe_args),
        };

        const {
          rows: [{ mbtiles_id }],
        } = await dama_meta.insertNewRow(
          "_data_manager_admin",
          "dama_views_mbtiles_metadata",
          newRow
        );

        const finalEvent = {
          type: "createDamaGisDatasetViewMbtiles:FINAL",
          payload: {
            view_id: dama_view_id,
            mbtiles_id,
            tippecanoeStdout,
            tippecanoeStderr,
          },
          meta: { etl_context_id, timestamp: now },
        };

        await dama_events.dispatch(finalEvent);

        return finalEvent.payload;
      } catch (err) {
        const { stack, message } = err as Error;

        const errorEvent = {
          type: "createDamaGisDatasetViewMbtiles:ERROR",
          payload: { message, stack },
          error: true,
          meta: { etl_context_id },
        };

        await dama_events.dispatch(errorEvent);

        throw err;
      }
    });
  }
}

export default new DamaGis();
