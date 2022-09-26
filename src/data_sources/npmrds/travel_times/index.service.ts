import { Context } from "moleculer";
import pgFormat from "pg-format";
import _ from "lodash";

import stageData from "../../../../tasks/NPMRDS_Database/src/npmrds/npmrds_travel_times/stage-npmrds-state-yrmo";
import publishData from "../../../../tasks/NPMRDS_Database/src/npmrds/npmrds_travel_times/publish-npmrds-state-yrmo";

// import { PgEnvSchema } from "../../../data_manager/dama_db/postgres/schemas";
import { stateAbbr2FipsCode } from "../../../data_utils/constants/stateFipsCodes";

import serviceNameBaseFromDataSourceName from "../../../data_manager/dama_utils/serviceNameBaseFromDataSourceName";

import damaViewMetadataBase from "../_shared_/damaViewMetadataBase";

export const dataSourceParentClass = "usdot/fhwa/npmrds";
export const dataSourceName = `${dataSourceParentClass}/travel_times`;

// npmrds/travel_times
export const serviceName = serviceNameBaseFromDataSourceName(dataSourceName);

export type TaskParams = {
  npmrds_export_sqlite_db_path: string;
  pgEnv: string;
};

export type EventMetadata = object & { pgEnv: string };

const commonDammaMetaProps = ["DAMAA", "etl_context_id"];

export default {
  name: serviceName,

  events: {
    [`${dataSourceParentClass}:UPDATE`]: {
      context: true,
      // params: FSAEventParam,
      async handler(ctx: Context) {
        const event = {
          // @ts-ignore
          ...ctx.params,
          type: `${dataSourceName}:LOAD_REQUEST`,
        };

        await ctx.emit(event);
      },
    },
    [`${dataSourceName}:LOAD_REQUEST`]: {
      context: true,
      // params: FSAEventParam,
      async handler(ctx: Context) {
        await this.stage(ctx);
      },
    },
    [`${dataSourceName}:LOADED`]: {
      context: true,
      // params: FSAEventParam,
      async handler(ctx: Context) {
        await this.requestQA(ctx);
      },
    },
    [`${dataSourceName}:QA_APPROVED`]: {
      context: true,
      // params: FSAEventParam,
      async handler(ctx: Context) {
        await this.makeViewMetadataTemplate(ctx);
      },
    },
    [`${dataSourceName}:VIEW_METADATA_SUBMITTED`]: {
      context: true,
      // params: FSAEventParam,
      async handler(ctx: Context) {
        await this.publish(ctx);
      },
    },
  },

  methods: {
    stage: {
      async handler(ctx: Context) {
        const { params: event } = ctx;

        const {
          // @ts-ignore
          payload: { npmrds_export_sqlite_db_path },
          // @ts-ignore
          meta: { etl_context_id: parent_context_id = null },
        } = event;

        const etl_context_id = await ctx.call(
          "dama_dispatcher.spawnDamaContext",
          { etl_context_id: parent_context_id }
        );

        const start_timestamp = new Date().toISOString();

        const payload: any = await stageData({
          npmrds_export_sqlite_db_path,
          // @ts-ignore
          pg_env: ctx.meta.pgEnv,
        });

        const loadedEvent = {
          type: `${dataSourceName}:LOADED`,
          payload,
          meta: {
            INITIAL: true,
            DAMAA: true,
            checkpoint: true,
            parent_context_id,
            etl_context_id,
            start_timestamp,
            end_timestamp: new Date().toISOString(),
          },
        };

        const damaEvent = await ctx.call(
          "dama_dispatcher.dispatch",
          loadedEvent
        );

        return damaEvent;
      },
    },

    requestQA: {
      async handler(ctx: Context) {
        const { params: event } = ctx;
        // @ts-ignore
        const { payload, meta } = event;

        const damaEvent = await ctx.call("dama_dispatcher.dispatch", {
          type: `${dataSourceName}:QA_REQUEST`,
          payload,
          meta: {
            ..._.pick(meta, commonDammaMetaProps),
            checkpoint: true,
            timestamp: new Date().toISOString(),
          },
        });

        return damaEvent;
      },
    },

    makeViewMetadataTemplate: {
      async handler(ctx: Context) {
        const { params: event } = ctx;
        const {
          // @ts-ignore
          payload: { state, year, month },
          // @ts-ignore
          meta: { etl_context_id, pgEnv },
        } = event;

        const mm = `0${month}`.slice(-2);

        const table_schema = state;
        const table_name = `npmrds_y${year}m${mm}`;
        const data_table = pgFormat("%I.%I", table_schema, table_name);
        const start_date = `${year}-01-01`;
        const end_date = `${year}-12-31`;

        const geography_version = stateAbbr2FipsCode[state.toLowerCase()];

        const type = `${dataSourceName}:VIEW_METADATA_TEMPLATE`;

        const payload = {
          ...damaViewMetadataBase,
          data_source_name: dataSourceName,
          data_type: "TABULAR",
          interval_version: "MONTH",
          version: "-1",
          geography_version,
          table_schema,
          table_name,
          data_table,
          start_date,
          end_date,
        };

        const meta = {
          DAMAA: true,
          etl_context_id,
          pgEnv,
          timestamp: new Date().toISOString(),
          checkpoint: true,
        };

        const e = {
          type,
          payload,
          meta,
        };

        const damaEvent = await ctx.call("dama_dispatcher.dispatch", e);

        return damaEvent;
      },
    },

    publish: {
      async handler(ctx: Context) {
        // @ts-ignore
        const {
          params: event,
          // @ts-ignore
          meta: { pgEnv },
        } = ctx;
        // @ts-ignore
        const { payload: oldPayload, meta } = event;

        // @ts-ignore
        const { table_schema, table_name } = oldPayload;

        const oldMeta = _.pick(meta, commonDammaMetaProps);

        const start_timestamp = new Date().toISOString();

        const publishResult = await publishData({
          table_schema,
          table_name,
          pg_env: pgEnv,
        });

        await ctx.call("dama_meta.updateDataManagerViewMetadata", event);

        const end_timestamp = new Date().toISOString();

        const type = `${dataSourceName}:PUBLISHED`;

        const e = {
          type,
          payload: publishResult,
          meta: {
            // @ts-ignore
            ...oldMeta,
            DAMAA: true,
            checkpoint: true,
            start_timestamp,
            end_timestamp,
          },
        };

        const damaEvent = await ctx.call("dama_dispatcher.dispatch", e);

        return damaEvent;
      },
    },
  },
};
