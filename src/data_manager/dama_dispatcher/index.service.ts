// @ts-ignore
import _ from "lodash";

import { Context } from "moleculer";

import { FSA } from "flux-standard-action";

export type ServiceContext = Context & {
  params: FSA;
};

export default {
  name: "dama_dispatcher",

  actions: {
    spawnDamaContext: {
      //  https://moleculer.services/docs/0.14/actions.html#Action-visibility
      //    public: public action, can be called locally & remotely but not published via API GW
      visibility: "public",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { etl_context_id = null, pg_env = "development" },
        } = ctx;

        const db = await this.broker.call("dama_db.getDb", pg_env);

        const q = `
          INSERT INTO _data_manager_admin.etl_context (
            parent_id
          ) VALUES ($1)
          RETURNING context_id
        `;

        const {
          rows: [{ context_id }],
        } = await db.query(q, [etl_context_id]);

        return context_id;
      },
    },

    dispatch: {
      visibility: "public",

      async handler(ctx: Context & { params: FSA }) {
        const { params } = ctx;
        const {
          meta: { DAMAA, etl_context_id, checkpoint, pg_env = "development" },
        } = params;

        let event = params;

        if (DAMAA) {
          if (!pg_env) {
            throw new Error("DAMAA events must include meta.pg_env");
          }

          console.log(
            "= ".repeat(10),
            "dama_dispatcher.dispatch",
            " =".repeat(10)
          );

          console.log(JSON.stringify(params, null, 4));
          console.log("= ".repeat(33));

          if (!etl_context_id) {
            throw new Error(
              "All Data Manager Action (DAMAA) Events MUST have a meta.etl_context_id"
            );
          }

          const db = await this.broker.call("dama_db.getDb", pg_env);

          // @ts-ignore
          const { type, payload, meta = null, error = null } = params;

          const q = `
            INSERT INTO _data_manager_admin.event_store_prototype (
              type,
              payload,
              meta,
              error,
              is_checkpoint
            ) VALUES ( $1, $2, $3, $4, $5 )
              RETURNING *
          `;

          const {
            rows: [damaEvent],
          } = await db.query(q, [type, payload, meta, error, !!checkpoint]);

          event = damaEvent;
        }

        // FIXME: Infinite Loop
        ctx.emit(params.type, event);

        return event;
      },
    },

    queryDamaEvents: {
      visibility: "public",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { etl_context_id, event_id, pg_env },
        } = ctx;

        const db = await this.broker.call("dama_db.getDb", pg_env);

        const q = `
          WITH RECURSIVE cte_ctx_tree(context_id, parent_id) AS (
            SELECT
                context_id,
                parent_id
              FROM _data_manager_admin.etl_context
              WHERE context_id = $1
            UNION    
            SELECT
                a.context_id,
                a.parent_id
              FROM _data_manager_admin.etl_context AS a
                INNER JOIN cte_ctx_tree
                  ON (
                    ( a.context_id = cte_ctx_tree.parent_id )
                    OR
                    ( a.parent_id = cte_ctx_tree.context_id )
                  )
        )
          SELECT
              event_id,
              type,
              payload,
              meta,
              error,
              is_checkpoint
            FROM _data_manager_admin.event_store_prototype AS a
              INNER JOIN cte_ctx_tree AS b
                ON (
                  ( ( meta->>'etl_context_id' )::INTEGER = b.context_id )
                )
            WHERE ( event_id > $2 )
            ORDER BY event_id
        `;

        const { rows: damaEvents } = await db.query(q, [
          etl_context_id,
          event_id,
        ]);

        return damaEvents;
      },
    },

    queryRootContextId: {
      visibility: "public",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { etl_context_id, pg_env },
        } = ctx;

        const db = await this.broker.call("dama_db.getDb", pg_env);

        const q = `
          WITH RECURSIVE cte_ctx_tree(context_id, parent_id) AS (
            SELECT
                context_id,
                parent_id
              FROM _data_manager_admin.etl_context
              WHERE context_id = $1
            UNION    
            SELECT
                a.context_id,
                a.parent_id
              FROM _data_manager_admin.etl_context AS a
                INNER JOIN cte_ctx_tree
                  ON (
                    ( a.context_id = cte_ctx_tree.parent_id )
                    OR
                    ( a.parent_id = cte_ctx_tree.context_id )
                  )
        )
          SELECT
              MIN(context_id) AS root_etl_context
            FROM cte_ctx_tree
        `;

        const {
          rows: [{ root_etl_context }],
        } = await db.query(q, [etl_context_id]);

        return root_etl_context;
      },
    },

    queryDamaEventById: {
      visibility: "public",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { event_id, pg_env },
        } = ctx;

        const db = await this.broker.call("dama_db.getDb", pg_env);

        const q = `
          SELECT
              event_id,
              type,
              payload,
              meta,
              error,
              is_checkpoint
            FROM _data_manager_admin.event_store_prototype
            WHERE ( event_id = $1 )
        `;

        const {
          rows: [damaEvent],
        } = await db.query(q, [event_id]);

        return damaEvent;
      },
    },
  },
};
