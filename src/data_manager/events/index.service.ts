// import { inspect } from "util";

import dedent from "dedent";
import _ from "lodash";

import { Context } from "moleculer";

import { FSA } from "flux-standard-action";

export type ServiceContext = Context & {
  params: FSA;
};

export default {
  name: "data_manager/events",

  actions: {
    spawnEtlContext: {
      visibility: "published",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { parent_context_id = null, source_id = null } = {},
        } = ctx;

        const sql = `
          INSERT INTO data_manager.etl_contexts (
            parent_context_id,
            source_id
          ) VALUES ($1, $2)
          RETURNING etl_context_id
        `;

        const {
          // @ts-ignore
          rows: [{ etl_context_id: newEtlContextId }],
        } = await ctx.call("dama_db.query", {
          text: sql,
          values: [parent_context_id, source_id],
        });

        return newEtlContextId;
      },
    },

    setEtlContextSourceId: {
      visibility: "published",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { etl_context_id, source_id = null },
        } = ctx;

        const sql = `
          UPDATE data_manager.etl_contexts
            SET source_id = $1
            WHERE etl_context_id = $2
        `;

        await ctx.call("dama_db.query", {
          text: sql,
          values: [source_id, etl_context_id],
        });
      },
    },

    dispatch: {
      visibility: "public",

      async handler(ctx: Context & { params: FSA }) {
        const { params } = ctx;

        console.log(JSON.stringify({ params, meta: ctx.meta }, null, 4));

        const etl_context_id =
          +params.meta?.etl_context_id || +ctx.meta?.etl_context_id;

        if (!etl_context_id) {
          throw new Error(
            "All Data Manager Action Events MUST have an etl_context_id."
          );
        }

        let event = params;

        /*
        // @ts-ignore
        event.meta = event.meta || {};
        // @ts-ignore
        event.meta.pgEnv = ctx.meta.pgEnv;
        */

        // @ts-ignore
        const { type, payload, meta = null, error = null } = params;

        const sql = dedent(`
          INSERT INTO data_manager.event_store (
            etl_context_id,
            type,
            payload,
            meta,
            error
          ) VALUES ( $1, $2, $3, $4, $5 )
            RETURNING *
        `);

        const {
          rows: [damaEvent],
          // @ts-ignore
        } = await ctx.call("dama_db.query", {
          text: sql,
          values: [
            etl_context_id,
            type,
            payload || null,
            meta || null,
            error || false,
          ],
        });

        event = damaEvent;

        // FIXME: Potential Infinite Loop if event handler re-dispatches.
        ctx.emit(params.type, event, { meta: ctx.meta });

        return event;
      },
    },

    queryEvents: {
      visibility: "public",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { etl_context_id, event_id },
          // @ts-ignore
          meta: { pgEnv },
        } = ctx;

        const sinceEventId = Number.isFinite(+event_id) ? +event_id : -1;

        const sql = dedent(`
          WITH RECURSIVE cte_ctx_tree(etl_context_id, parent_context_id) AS (
            SELECT
                etl_context_id,
                parent_context_id
              FROM data_manager.etl_contexts
              WHERE etl_context_id = $1
            UNION    
            SELECT
                a.etl_context_id,
                a.parent_context_id
              FROM data_manager.etl_contexts AS a
                INNER JOIN cte_ctx_tree
                  ON (
                    ( a.etl_context_id = cte_ctx_tree.parent_context_id )
                    OR
                    ( a.parent_context_id = cte_ctx_tree.etl_context_id )
                  )
          )
            SELECT
                event_id,
                etl_context_id,
                type,
                payload,
                meta,
                error
              FROM data_manager.event_store AS a
                INNER JOIN cte_ctx_tree AS b
                  USING (etl_context_id)
              WHERE ( event_id > $2 )
              ORDER BY event_id
        `);

        // @ts-ignore
        const { rows: damaEvents } = await ctx.call("dama_db.query", {
          text: sql,
          values: [etl_context_id, sinceEventId],
        });

        damaEvents.forEach((e: FSA) => {
          // @ts-ignore
          e.meta = e.meta || {};
          // @ts-ignore
          e.meta.pgEnv = pgEnv;
        });

        return damaEvents;
      },
    },

    queryRootContextId: {
      visibility: "public",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { etl_context_id },
        } = ctx;

        const sql = dedent(`
          WITH RECURSIVE cte_ctx_tree(etl_context_id, parent_context_id) AS (
            SELECT
                etl_context_id,
                parent_context_id
              FROM data_manager.etl_contexts
              WHERE etl_context_id = $1
            UNION    
            SELECT
                a.etl_context_id,
                a.parent_context_id
              FROM data_manager.etl_contexts AS a
                INNER JOIN cte_ctx_tree
                  ON (
                    ( a.etl_context_id = cte_ctx_tree.parent_context_id )
                    OR
                    ( a.parent_context_id = cte_ctx_tree.etl_context_id )
                  )
        )
          SELECT
              MIN(etl_context_id) AS root_etl_context
            FROM cte_ctx_tree
        `);

        const {
          rows: [{ root_etl_context }],
          // @ts-ignore
        } = await ctx.call("dama_db.query", {
          text: sql,
          values: [etl_context_id],
        });

        return root_etl_context;
      },
    },

    getEventById: {
      visibility: "public",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { event_id },
        } = ctx;

        const sql = dedent(`
          SELECT
              event_id,
              etl_context_id,
              type,
              payload,
              meta,
              error
            FROM data_manager.event_store
            WHERE ( event_id = $1 )
        `);

        const {
          rows: [damaEvent],
          // @ts-ignore
        } = await ctx.call("dama_db.query", { text: sql, values: [event_id] });

        return damaEvent;
      },
    },
  },
};
