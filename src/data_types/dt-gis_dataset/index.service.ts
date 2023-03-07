import { Context } from "moleculer";
import uploadFile from './upload/upload'
import publish from './publish/publish'
import { createViewMbtiles } from  './mbtiles/mbtiles'
import dedent from "dedent";
import pgFormat from "pg-format";
import _ from 'lodash';

import GeospatialDatasetIntegrator from "../../../tasks/gis-data-integration/src/data_integrators/GeospatialDatasetIntegrator";

export const serviceName = 'gis-dataset'


export default {
  name: serviceName,
  actions: {
    
    //uploads a file and unzips it into tmp-etl
    uploadFile,
  
    //these routes get data from fs based on id passed to client in upload route 
    getLayerNames(ctx) {
      const {
        // @ts-ignore
        params: { id },
      } = ctx;

      const gdi = new GeospatialDatasetIntegrator(id);

      // @ts-ignore
      const layerNameToId = gdi.layerNameToId;
      const layerNames = Object.keys(layerNameToId);

      return layerNames;
    },

    async getTableDescriptor(ctx) {
      const {
        // @ts-ignore
        params: { id, layerName },
      } = ctx;

      const gdi = new GeospatialDatasetIntegrator(id);

      const tableDescriptor = await gdi.getLayerTableDescriptor(layerName);

      return tableDescriptor;
    },

    async getLayerAnalysis(ctx) {
      const {
        // @ts-ignore
        params: { id, layerName },
      } = ctx;

      const gdi = new GeospatialDatasetIntegrator(id);

      const layerAnalysis = await gdi.getGeoDatasetLayerAnalysis(layerName);

      return layerAnalysis;
    },

    publish,

    // -----------------------------------------------
    //-- MBTILES functions
    //-- to do: remove dama_admin views and simplify
    // -----------------------------------------------
    createViewMbtiles,

    getDamaGisDatasetViewTableSchemaSummary: {
      async handler(ctx) {
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
        console.log('getDamaGisDatasetViewTableSchemaSummary', damaViewId)
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

    async generateGisDatasetViewGeoJsonSqlQuery(ctx) {
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

    makeDamaGisDatasetViewGeoJsonFeatureAsyncIterator: {
      visibility: "protected",

      async *handler(ctx) {
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
    }

  }
}