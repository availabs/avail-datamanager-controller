import _ from "lodash";

import { Context } from "moleculer";

import { FSA } from "flux-standard-action";

export type ServiceContext = Context & {
  params: FSA;
};

export default {
  name: "dama_meta",

  actions: {
    updateDataManagerViewMetadata: {
      visibility: "public",

      async handler(ctx: Context) {
        const {
          params: {
            payload,
            meta: { etl_context_id },
          },
        } = ctx;

        const db = await ctx.call("dama_db.getDb");

        const root_etl_context_id = await ctx.call(
          "dama_dispatcher.queryRootContextId",
          { etl_context_id }
        );

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

        const { rows: colsQRows } = await db.query(colsQ);

        const columnNames = colsQRows.map(({ column_name }) => column_name);

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
            cols.push(k);
            const i = queryParams.push(viewMeta[k]);

            if (k === "last_updated") {
              return `$${i}::TIMESTAMP AS ${k}`;
            }

            if (/_date$/.test(k)) {
              return `$${i}::DATE AS ${k}`;
            }

            return `$${i} AS ${k}`;
          })
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
              WHERE ( name = $${queryParams.push(
                data_source_name.toUpperCase()
              )} )
            RETURNING id
          ;
        `;

        const {
          rows: [{ id: dama_view_id }],
        } = await db.query(insertQ, queryParams);

        return { dama_view_id };
      },
    },
  },
};
