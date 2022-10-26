import _ from "lodash";

import { Context } from "moleculer";

import dedent from "dedent";
import pgFormat from "pg-format";

import { FSA } from "flux-standard-action";

export type ServiceContext = Context & {
  params: FSA;
};

const serviceName = "dama/metadata";

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
        const { dataSourceViewId } = ctx.params;

        const text = dedent(`
          SELECT
              a.column_name
            FROM information_schema.columns AS a
              INNER JOIN data_manager.views AS b
                USING ( table_schema, table_name )
            WHERE ( b.id = $1 )
            ORDER BY a.ordinal_position
          ;
        `);

        const { rows } = await ctx.call("dama_db.query", {
          text,
          values: [dataSourceViewId],
        });

        const columnNames = rows.map(({ column_name }) => column_name);

        return columnNames;
      },
    },

    getDataSourceMaxViewId: {
      visibility: "public",

      async handler(ctx: Context) {
        // @ts-ignore
        const { dataSourceId } = ctx.params;

        const text = dedent(`
          SELECT
              MAX(id) AS latest_view_id
            FROM data_manager.views
            WHERE ( source_id = $1 )
          ;
        `);

        const { rows } = await ctx.call("dama_db.query", {
          text,
          values: [dataSourceId],
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
        // getDataSourceMaxViewId has same params
        const dataSourceViewId = await this.actions.getDataSourceMaxViewId(
          ctx.params,
          { parentCtx: ctx }
        );

        const columnNames =
          await this.actions.getDataSourceViewDataTableColumnsNames(
            { dataSourceViewId },
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
          last_updated: new Date().toISOString(),
        };

        const cols: string[] = [];
        const queryParams: any[] = [];

        const selectClauses = Object.keys(viewMeta)
          .map((k) => {
            if (k === "id" || k === "source_id") {
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
                a.id AS source_id,
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
          rows: [{ id: dama_view_id }],
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
      visibility: "public",

      async handler(ctx: Context) {
        const q = dedent(`
          SELECT
              id,
              name
            FROM data_manager.sources
            ORDER BY 1
          ;
        `);

        const { rows } = await ctx.call("dama_db.query", q);

        return rows;
      },
    },

    createNewDataSource: {
      visibility: "published",

      async handler(ctx: Context) {
        const newSrcProps = <object>ctx.params;

        const tableCols = <string[]>await this.actions.getTableColumns(
          {
            tableSchema: "data_manager",
            tableName: "sources",
          },
          { parentCtx: ctx }
        );

        const props = Object.keys(newSrcProps);
        const cols = _.pull(_.intersection(tableCols, props), "id");

        const colTags = cols.map(() => "%I");
        const params = _.range(1, cols.length + 1).map((i) => `$${i}`);
        // @ts-ignore
        const values = cols
          .map((c) => newSrcProps[c])
          .map((v) => (v === "" ? null : v));

        const text = dedent(
          pgFormat(
            `
            INSERT INTO data_manager.sources (
                ${colTags}
              ) VALUES (${params})
              RETURNING *
            ;
          `,
            ...cols
          )
        );

        const {
          rows: [damaSrcMeta],
        } = await ctx.call("dama_db.query", { text, values });

        return damaSrcMeta;
      },
    },
  },
};
