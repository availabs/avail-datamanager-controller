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
            meta: { etl_context_id, pg_env = "development" },
          },
        } = ctx;

        const db = await this.broker.call("dama_db.getDb", pg_env);

        console.log("==> GOT DB ".repeat(20));

        const root_etl_context_id = await this.broker.call(
          "dama_dispatcher.queryRootContextId",
          { pg_env, etl_context_id }
        );

        const colsQ = `
          SELECT
              column_name
            FROM information_schema.columns
            WHERE (
              ( table_schema = '_data_manager_admin' )
              AND
              ( table_name = 'data_views_proto' )
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
          INSERT INTO _data_manager_admin.data_views_proto (
            source_id, ${cols.join(", ")}
          )
            SELECT
                a.id AS source_id,
                ${selectClauses}
              FROM _data_manager_admin.data_sources_proto AS a
              WHERE ( name = $${queryParams.push(data_source_name)} )
            RETURNING id
          ;
        `;

        console.log(insertQ);
        const {
          rows: [{ id: dama_view_id }],
        } = await db.query(insertQ, queryParams);

        return { dama_view_id };
      },
    },
  },
};
