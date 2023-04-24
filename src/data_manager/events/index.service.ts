import { Context } from "moleculer";

import dama_events, { EtlEvent } from ".";

export default {
  name: "data_manager/events",

  actions: {
    spawnEtlContext: {
      visibility: "published",

      handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { source_id = null, parent_context_id = null } = {},
        } = ctx;

        return dama_events.spawnEtlContext(source_id, parent_context_id);
      },
    },

    setEtlContextSourceId: {
      visibility: "published",

      handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { etl_context_id, source_id = null },
        } = ctx;

        return dama_events.setEtlContextSourceId(etl_context_id, source_id);
      },
    },

    dispatch: {
      visibility: "public",

      handler(ctx: Context & { params: EtlEvent }) {
        const { params } = ctx;

        const etl_context_id =
          // @ts-ignore
          +params.meta?.etl_context_id || +ctx.meta?.etl_context_id;

        return dama_events.dispatch(params, etl_context_id);
      },
    },

    queryEvents: {
      visibility: "public",

      handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { etl_context_id, event_id },
        } = ctx;

        return dama_events.queryEvents(+event_id || -1, etl_context_id);
      },
    },

    queryOpenEtlProcessesStatusUpdatesForService(ctx: Context) {
      const {
        // @ts-ignore
        params: { serviceName },
      } = ctx;

      return dama_events.queryOpenEtlProcessesStatusUpdatesForService(
        serviceName
      );
    },

    queryEtlContextFinalEvent(ctx: Context) {
      const {
        // @ts-ignore
        params: { etlContextId },
      } = ctx;

      return dama_events.queryEtlContextFinalEvent(etlContextId);
    },
  },
};
