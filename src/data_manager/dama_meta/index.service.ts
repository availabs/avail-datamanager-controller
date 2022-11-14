import _ from "lodash";

import { Context } from "moleculer";

import dedent from "dedent";
import pgFormat from "pg-format";

import { FSA } from "flux-standard-action";

import serviceName from "./constants/serviceName";

import EventTypes from "./constants/EventTypes";

export type ServiceContext = Context & {
  params: FSA;
};

export default {
  name: serviceName,

  actions: {
    getTableColumns: {
      visibility: "public",

      async handler(ctx: Context) {
        // @ts-ignore
        const { tableSchema, tableName } = ctx.params;

        const text = dedent(`
          SELECT
              column_name
            FROM information_schema.columns
            WHERE (
              ( table_schema = $1 )
              AND
              ( table_name = $2 )
            )
            ORDER BY ordinal_position
          ;
        `);

        const { rows } = await ctx.call("dama_db.query", {
          text,
          values: [tableSchema, tableName],
        });

        const columnNames = rows.map(({ column_name }) => column_name);

        return columnNames;
      },
    },

    getDataSourceViewDataTableColumnsNames: {
      visibility: "public",

      async handler(ctx: Context) {
        // @ts-ignore
        const { damaViewId } = ctx.params;

        const text = dedent(`
          SELECT
              a.column_name
            FROM information_schema.columns AS a
              INNER JOIN data_manager.views AS b
                USING ( table_schema, table_name )
            WHERE ( b.view_id = $1 )
            ORDER BY a.ordinal_position
          ;
        `);

        const { rows } = await ctx.call("dama_db.query", {
          text,
          values: [damaViewId],
        });

        const columnNames = rows.map(({ column_name }) => column_name);

        return columnNames;
      },
    },

    getDataSourceMaxViewId: {
      visibility: "public",

      async handler(ctx: Context) {
        // @ts-ignore
        const { damaSourceId } = ctx.params;

        const text = dedent(`
          SELECT
              MAX(view_id) AS latest_view_id
            FROM data_manager.views
            WHERE ( source_id = $1 )
          ;
        `);

        const { rows } = await ctx.call("dama_db.query", {
          text,
          values: [damaSourceId],
        });

        if (rows.length < 1) {
          return null;
        }

        const [{ latest_view_id }] = rows;

        return latest_view_id;
      },
    },

    getDataSourceLatestViewTableColumns: {
      visibility: "published",

      async handler(ctx: Context) {
        console.log(
          JSON.stringify({ params: ctx.params, meta: ctx.meta }, null, 4)
        );

        // getDataSourceMaxViewId has same params
        const damaViewId = await this.actions.getDataSourceMaxViewId(
          ctx.params,
          { parentCtx: ctx }
        );

        console.log(
          JSON.stringify(
            { params: ctx.params, meta: ctx.meta, damaViewId },
            null,
            4
          )
        );

        const columnNames =
          await this.actions.getDataSourceViewDataTableColumnsNames(
            { damaViewId },
            { parentCtx: ctx }
          );

        return columnNames;
      },
    },

    getInsertDataManagerViewMetadataSql: {
      visibility: "public",

      async handler(ctx: Context) {
        const {
          params: {
            // @ts-ignore
            payload,
            // @ts-ignore
            meta: { etl_context_id },
          },
        } = ctx;

        const columnNames: string[] = await this.actions.getTableColumns(
          { tableSchema: "data_manager", tableName: "views" },
          { parentCtx: ctx }
        );

        const root_etl_context_id = await ctx.call(
          "dama_dispatcher.queryRootContextId",
          { etl_context_id }
        );

        const viewMeta = {
          ..._.pick(payload, columnNames),
          root_etl_context_id,
          etl_context_id,
        };

        const cols: string[] = [];
        const queryParams: any[] = [];

        const selectClauses = Object.keys(viewMeta)
          .map((k) => {
            if (k === "view_id" || k === "source_id") {
              return null;
            }

            cols.push(k);

            let val = viewMeta[k];

            if (val === "") {
              val = null;
            }

            const i = queryParams.push(val);

            if (k === "last_updated") {
              return `$${i}::TIMESTAMP AS ${k}`;
            }

            if (/_date$/.test(k)) {
              return `$${i}::DATE AS ${k}`;
            }

            return `$${i} AS ${k}`;
          })
          .filter(Boolean)
          .join(", ");

        const { data_source_name } = payload;

        const insertQ = `
          INSERT INTO data_manager.views (
            source_id, ${cols.join(", ")}
          )
            SELECT
                a.source_id,
                ${selectClauses}
              FROM data_manager.sources AS a
              WHERE ( name = $${queryParams.push(data_source_name)} )
            RETURNING *
          ;
        `;

        return {
          text: dedent(insertQ),
          values: queryParams,
        };
      },
    },

    updateDataManagerViewMetadata: {
      visibility: "public",

      async handler(ctx: Context) {
        const query = await this.actions.getInsertDataManagerViewMetadataSql(
          ctx.params,
          { parentCtx: ctx }
        );

        const {
          rows: [{ view_id: dama_view_id }],
        } = await ctx.call("dama_db.query", query);

        return { dama_view_id };
      },
    },

    getTableJsonSchema: {
      visibility: "public",

      async handler(ctx: Context) {
        // @ts-ignore
        const { tableSchema, tableName } = ctx.params;

        const text = dedent(`
          SELECT
              t.schema
            FROM _data_manager_admin.table_schema_as_json_schema($1, $2) AS t(schema)
          ;
        `);

        const {
          rows: [{ schema }],
        } = await ctx.call("dama_db.query", {
          text,
          values: [tableSchema, tableName],
        });

        return schema;
      },
    },

    getDamaDataSources: {
      visibility: "published",

      async handler(ctx: Context) {
        const q = dedent(`
          SELECT
              source_id,
              name
            FROM data_manager.sources
            ORDER BY 1
          ;
        `);

        const { rows } = await ctx.call("dama_db.query", q);

        return rows;
      },
    },

    async getDamaViewProperties(ctx: Context) {
      const {
        // @ts-ignore
        params: { damaViewId, properties },
      } = ctx;

      const formatObj = properties.reduce(
        (acc: any, prop: string) => {
          acc.formatTypes.push("%I");
          acc.formatValues.push(prop);
          return acc;
        },
        { formatTypes: [], formatValues: [] }
      );

      const q = dedent(
        pgFormat(
          `
            SELECT
                ${formatObj.formatTypes}
              FROM _data_manager_admin.dama_views_comprehensive
              WHERE ( view_id = $1 )
          `,
          ...formatObj.formatValues
        )
      );

      const { rows } = await ctx.call("dama_db.query", {
        text: q,
        values: [damaViewId],
      });

      if (rows.length !== 1) {
        throw new Error(`Invalid DamaViewID ${damaViewId}`);
      }

      return rows[0];
    },

    async getDamaViewName(ctx: Context) {
      const {
        // @ts-ignore
        params: { damaViewId },
      } = ctx;

      const { dama_view_name } = await this.actions.getDamaViewProperties(
        { damaViewId, properties: ["dama_view_name"] },
        { parentCtx: ctx }
      );

      return dama_view_name;
    },

    async getDamaViewGlobalId(ctx: Context) {
      const {
        // @ts-ignore
        params: { damaViewId },
      } = ctx;

      const { dama_global_id } = await this.actions.getDamaViewProperties(
        { damaViewId, properties: ["dama_global_id"] },
        { parentCtx: ctx }
      );

      return dama_global_id;
    },

    async generateCreateDamaSourceSql(ctx: Context) {
      return await ctx.call("dama_db.generateInsertStatement", {
        tableSchema: "data_manager",
        tableName: "sources",
        newRow: ctx.params,
      });
    },

    createNewDamaSource: {
      visibility: "published",

      async handler(ctx: Context) {
        const q = await this.actions.generateCreateDamaSourceSql(ctx.params, {
          parentCtx: ctx,
        });

        const {
          rows: [newDamaSource],
        } = await ctx.call("dama_db.query", q);

        return newDamaSource;
      },
    },

    async queueEtlCreateDamaSource(ctx: Context) {
      // @ts-ignore
      const { params }: { params: object } = ctx;

      // @ts-ignore
      const { etlContextId } = params;

      if (!etlContextId) {
        throw new Error("The etlContextId parameter is required.");
      }

      const payload = _.omit(params, ["etlContextId"]);

      const event = {
        type: EventTypes.QUEUE_CREATE_NEW_DAMA_SOURCE,
        payload,
        meta: {
          etl_context_id: etlContextId,
          timestamp: new Date().toISOString(),
        },
      };

      const result = await ctx.call("dama_dispatcher.dispatch", event);

      return result;
    },

    generateCreateDamaViewSql: {
      visibility: "public",

      async handler(ctx: Context) {
        // @ts-ignore
        const { params }: { params: object } = ctx;

        // @ts-ignore
        let { etl_context_id } = params;

        if (!etl_context_id) {
          // @ts-ignore
          etl_context_id = ctx.meta.etl_context_id || null;
        }

        const root_etl_context_id = etl_context_id
          ? await ctx.call("dama_dispatcher.queryRootContextId", {
              etl_context_id,
            })
          : null;

        const newRow = {
          ...params,
          root_etl_context_id,
          etl_context_id,
        };

        return await ctx.call("dama_db.generateInsertStatement", {
          tableSchema: "data_manager",
          tableName: "views",
          newRow,
        });
      },
    },

    createNewDamaView: {
      visibility: "published",

      async handler(ctx: Context) {
        const q = await this.actions.generateCreateDamaViewSql(ctx.params, {
          parentCtx: ctx,
        });

        const {
          rows: [damaSrcMeta],
        } = await ctx.call("dama_db.query", q);

        return damaSrcMeta;
      },
    },

    async queueEtlCreateDamaView(ctx: Context) {
      // @ts-ignore
      const { params }: { params: object } = ctx;

      // @ts-ignore
      const { etlContextId } = params;

      if (!etlContextId) {
        throw new Error("The etlContextId parameter is required.");
      }

      const payload = _.omit(params, ["etlContextId"]);

      const event = {
        type: EventTypes.QUEUE_CREATE_NEW_DAMA_VIEW,
        payload,
        meta: {
          etl_context_id: etlContextId,
          timestamp: new Date().toISOString(),
        },
      };

      const result = await ctx.call("dama_dispatcher.dispatch", event);

      return result;
    },
  },
};
