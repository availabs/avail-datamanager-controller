import _ from "lodash";

import { Context } from "moleculer";

import dedent from "dedent";
import pgFormat from "pg-format";

import { FSA } from "flux-standard-action";

export type ServiceContext = Context & {
  params: FSA;
};

export const serviceName = "dama/metadata";

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

    getDamaSourceIdForName: {
      visibility: "public",

      async handler(ctx: Context) {
        // @ts-ignore
        const { damaSourceName } = ctx.params;

        console.log("==> damaSourceName:", damaSourceName);

        const text = dedent(`
          SELECT
              source_id
            FROM data_manager.sources
            WHERE ( name = $1 )
          ;
        `);

        const {
          // @ts-ignore
          rows: [{ source_id = null } = {}],
        } = await ctx.call("dama_db.query", {
          text,
          values: [damaSourceName],
        });

        if (source_id === null) {
          throw new Error(`No DamaSource with name ${damaSourceName}.`);
        }

        return source_id;
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

        const viewMeta = {
          ..._.pick(payload, columnNames),
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

        const sql = `
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
          text: dedent(sql),
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
        const sql = dedent(`
          SELECT
              source_id,
              name
            FROM data_manager.sources
            ORDER BY 1
          ;
        `);

        const { rows } = await ctx.call("dama_db.query", sql);

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

      const sql = dedent(
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
        text: sql,
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

    async getDamaViewNamePrefix(ctx: Context) {
      const {
        // @ts-ignore
        params: { damaViewId },
      } = ctx;

      const { dama_view_name_prefix } =
        await this.actions.getDamaViewProperties(
          { damaViewId, properties: ["dama_view_name_prefix"] },
          { parentCtx: ctx }
        );

      return dama_view_name_prefix;
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

    async getDamaViewMapboxPaintStyle(ctx: Context) {
      const {
        // @ts-ignore
        params: { damaViewId },
      } = ctx;

      const text = dedent(`
        SELECT
            mapbox_paint_style
          FROM _data_manager_admin.dama_views_mapbox_comprehensive
          WHERE ( view_id = $1 )
      `);

      const { rows } = await ctx.call("dama_db.query", {
        text,
        values: [damaViewId],
      });

      if (rows.length === 0) {
        throw new Error(`Invalid DamaViewId: ${damaViewId}`);
      }

      const [{ mapbox_paint_style }] = rows;

      return mapbox_paint_style;
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
        const sql = await this.actions.generateCreateDamaSourceSql(ctx.params, {
          parentCtx: ctx,
        });

        const {
          rows: [newDamaSource],
        } = await ctx.call("dama_db.query", sql);

        return newDamaSource;
      },
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

        const newRow = {
          ...params,
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
        const sql = await this.actions.generateCreateDamaViewSql(ctx.params, {
          parentCtx: ctx,
        });

        const {
          rows: [damaSrcMeta],
        } = await ctx.call("dama_db.query", sql);

        return damaSrcMeta;
      },
    },

    deleteDamaView: {
      // remove a view entry. ideally should keep track of deletes with userids, and etl ids.
      visibility: "published",

      async handler(ctx: Context) {
        const sql = `DELETE FROM data_manager.views where view_id = ${ctx.params.view_id}`;

        const res = await ctx.call("dama_db.query", sql);

        console.log("res", res);

        return sql;
      },
    },

    deleteDamaSource: {
      // remove a view entry. ideally should keep track of deletes with userids, and etl ids.
      visibility: "published",

      async handler(ctx: Context) {
        const deleteViews = `DELETE FROM data_manager.views where source_id = ${ctx.params.source_id}`;
        const deleteSource = `DELETE FROM data_manager.sources where source_id = ${ctx.params.source_id}`;

        await ctx.call("dama_db.query", deleteViews);
        const res = await ctx.call("dama_db.query", deleteSource);

        console.log("res", res);

        return res;
      },
    },

    makeAuthoritativeDamaView: {
      // update view meta, update other views of the source too.
      async handler(ctx: Context) {
        const makeViewAuthSql = `
              UPDATE data_manager.views
              set metadata = CASE WHEN metadata is null THEN '{"authoritative": "true"}' ELSE metadata::text::jsonb || '{"authoritative": "true"}'  END
              where view_id = ${ctx.params.view_id};
        `;

        const invalidateOtherViewsSql = `
              UPDATE data_manager.views
              set metadata = metadata || '{"authoritative": "false"}'
              where source_id IN (select source_id from data_manager.views where view_id = ${ctx.params.view_id})
              and view_id != ${ctx.params.view_id};`;

        await ctx.call("dama_db.query", makeViewAuthSql);
        await ctx.call("dama_db.query", invalidateOtherViewsSql);

        return "success";
      },
    },

    async getDamaSourceMetadataByName(ctx: Context) {
      const {
        params: { damaSourceNames },
      } = ctx;

      const opts = { parentCtx: ctx };

      const sql = dedent(`
        SELECT
            *
          FROM data_manager.sources
          WHERE ( name = ANY($1) )
      `);

      // @ts-ignore
      const { rows } = await ctx.call(
        "dama_db.query",
        {
          text: sql,
          values: [damaSourceNames],
        },
        opts
      );

      const rowsByName = rows.reduce((acc: Record<string, any>, row: any) => {
        const { name } = row;

        acc[name] = row;

        return acc;
      }, {});

      // @ts-ignore
      const metaByName = damaSourceNames.reduce(
        (acc: Record<string, any>, name: string) => {
          acc[name] = rowsByName[name] || null;

          return acc;
        },
        {}
      );

      return metaByName;
    },
  },
};
