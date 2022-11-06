// FIXME: Need subEtlContexts for each layer

import { existsSync } from "fs";
import { readdir as readdirAsync } from "fs/promises";
import { join } from "path";

import { Context } from "moleculer";

import pgFormat from "pg-format";
import dedent from "dedent";
import _ from "lodash";

import etlDir from "../../constants/etlDir";

import serviceName from "./constants/serviceName";

export default {
  name: serviceName,

  actions: {
    getDamaGisDatasetViewTableSchemaSummary: {
      async handler(ctx: Context) {
        const {
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

        const damaViewIntIdRes = await ctx.call("dama_db.query", {
          text: damaViewIntIdQ,
          values: [damaViewId],
        });

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

    // List of the GIS Dataset uploads in the etl-work-dir directory.
    makeDamaGisDatasetViewGeoJsonFeatureIterator: {
      visibility: "protected",

      async handler(ctx: Context) {
        const {
          params: { damaViewId, config = {} },
        } = ctx;

        console.log(JSON.stringify(ctx.params, null, 4));

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

        let props = [];

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
          { placeholders: [], values: [] }
        );

        const selectColsClause = _.uniq([
          intIdColName,
          geometryColumn,
          ...props,
        ])
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

        const iter = await ctx.call("dama_db.makeIterator", {
          query: sql,
          config,
        });

        return iter;
      },
    },

    async testIter(ctx: Context) {
      const iter =
        await this.actions.makeDamaGisDatasetViewGeoJsonFeatureIterator(
          ctx.params,
          { parentCtx: ctx }
        );

      for await (const feature of iter) {
        console.log(JSON.stringify(feature, null, 4));
      }
    },
  },
};
