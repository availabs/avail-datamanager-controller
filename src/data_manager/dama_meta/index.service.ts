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
    getUpdateDataManagerViewMetadataSql: {
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

        const colsQ = `
          SELECT
              column_name
            FROM information_schema.columns
            WHERE (
              ( table_schema = 'data_manager' )
              AND
              ( table_name = 'views' )
            )
        ;`;

        const db = await ctx.call("dama_db.getDb");

        // @ts-ignore
        const { rows: colsQRows } = await db.query(colsQ);

        const columnNames = colsQRows.map(({ column_name }) => column_name);

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

        console.log(JSON.stringify({ viewMeta }, null, 4));

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
            RETURNING id
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
        const query = await ctx.call(
          "dama/metadata.getUpdateDataManagerViewMetadataSql",
          ctx.params
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

        console.log(ctx.params);

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

        console.log(JSON.stringify({ schema }, null, 4));

        return schema;
      },
    },

    getTableColumns: {
      visibility: "public",

      async handler(ctx: Context) {
        // @ts-ignore
        const { tableSchema, tableName } = ctx.params;

        console.log("==> tableSchema:", tableSchema, "; tableName:", tableName);

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

        const tableCols = <string[]>await ctx.call(
          "dama/metadata.getTableColumns",
          {
            tableSchema: "data_manager",
            tableName: "sources",
          }
        );

        const props = Object.keys(newSrcProps);
        const cols = _.pull(_.intersection(tableCols, props), "id");

        console.log(JSON.stringify({ tableCols, props, cols }, null, 4));

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
