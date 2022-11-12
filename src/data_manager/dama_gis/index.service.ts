import { rename as renameAsync } from "fs/promises";
import { join, basename } from "path";

import { Context } from "moleculer";

import pgFormat from "pg-format";
import dedent from "dedent";
import _ from "lodash";
import tmp from "tmp";

import asyncGeneratorToNdjsonStream from "../../data_utils/streaming/asyncGeneratorToNdjsonStream";

import { NodePgQueryResult } from "../dama_db/postgres/PostgreSQL";

import etlDir from "../../constants/etlDir";

import { getTimestamp } from "../../data_utils/time";

import createMbtilesTask from "../../../tasks/create-mbtiles";

import mbtilesDir from "../../constants/mbtilesDir";

import serviceName from "./constants/serviceName";
import { timeStamp } from "console";

export default {
  name: serviceName,

  actions: {
    getDamaGisDatasetViewTableSchemaSummary: {
      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { damaViewId },
        } = ctx;

        const damaViewPropsColsQ = dedent(`
          SELECT
              column_name,
              is_geometry_col
            FROM _data_manager_admin.dama_table_column_types
            WHERE (
              ( view_id = $1 )
            )
        `);

        const { rows } = await ctx.call("dama_db.query", {
          text: damaViewPropsColsQ,
          values: [damaViewId],
        });

        if (rows.length === 0) {
          throw new Error(`Invalid DamaViewId: ${damaViewId}`);
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

        const damaViewIntIdRes: NodePgQueryResult = await ctx.call(
          "dama_db.query",
          {
            text: damaViewIntIdQ,
            values: [damaViewId],
          }
        );

        if (!damaViewIntIdRes.rows.length) {
          throw new Error(
            `Unable to get primary key metadata for DamaView ${damaViewId}`
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
      },
    },

    async generateGisDatasetViewGeoJsonSqlQuery(ctx: Context) {
      const {
        // @ts-ignore
        params: { damaViewId, config = {} },
      } = ctx;

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
      } = await this.actions.getDamaGisDatasetViewTableSchemaSummary(
        { damaViewId },
        { parentCtx: ctx }
      );

      if (!geometryColumn) {
        throw new Error(
          `DamaView's ${damaViewId} does not appear to be a GIS dataset.`
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
    },

    // List of the GIS Dataset uploads in the etl-work-dir directory.
    makeDamaGisDatasetViewGeoJsonFeatureAsyncIterator: {
      visibility: "protected",

      async *handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { config = {} },
        } = ctx;

        const sql = await this.actions.generateGisDatasetViewGeoJsonSqlQuery(
          ctx.params,
          { parentCtx: ctx }
        );

        const iter = <AsyncGenerator<any>>await ctx.call(
          "dama_db.makeIterator",
          {
            query: sql,
            config,
          }
        );

        for await (const { feature } of iter) {
          yield feature;
        }
      },
    },

    makeDamaGisDatasetViewGeoJsonlStream: {
      visibility: "published",

      async handler(ctx: Context) {
        const iter =
          await this.actions.makeDamaGisDatasetViewGeoJsonFeatureAsyncIterator(
            ctx.params,
            { parentCtx: ctx }
          );

        return asyncGeneratorToNdjsonStream(iter);
      },
    },

    createDamaGisDatasetViewMbtiles: {
      visibility: "published",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { damaViewId },
        } = ctx;

        const { path: etlWorkDir, cleanupCallback: eltWorkDirCleanup } =
          await new Promise((resolve, reject) =>
            tmp.dir({ tmpdir: etlDir }, (err, path, cleanupCallback) => {
              if (err) {
                return reject(err);
              }

              resolve({ path, cleanupCallback });
            })
          );

        const [damaViewName, damaViewGlobalId] = await Promise.all([
          ctx.call("dama/metadata.getDamaViewName", {
            damaViewId,
          }),

          ctx.call("dama/metadata.getDamaViewGlobalId", {
            damaViewId,
          }),
        ]);

        const ts = getTimestamp();
        const mbtilesFileName = `${damaViewGlobalId}_${ts}.mbtiles`;
        const mbtilesFilePath = join(etlWorkDir, mbtilesFileName);

        const featuresAsyncIterator =
          await this.actions.makeDamaGisDatasetViewGeoJsonFeatureAsyncIterator(
            ctx.params,
            { parentCtx: ctx }
          );

        await createMbtilesTask({
          layerName: <string>damaViewName,
          mbtilesFilePath,
          featuresAsyncIterator,
          etlWorkDir,
        });

        const mbtilesBaseName = basename(mbtilesFilePath);
        const servedMbtilesPath = join(mbtilesDir, mbtilesBaseName);

        await renameAsync(mbtilesFilePath, servedMbtilesPath);
        await eltWorkDirCleanup();
      },
    },

    async testIter(ctx: Context) {
      const iter =
        await this.actions.makeDamaGisDatasetViewGeoJsonFeatureAsyncIterator(
          ctx.params,
          { parentCtx: ctx }
        );

      for await (const feature of iter) {
        console.log(JSON.stringify(feature, null, 4));
      }
    },
  },
};
