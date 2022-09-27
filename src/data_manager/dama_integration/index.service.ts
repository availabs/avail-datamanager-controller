import { existsSync } from "fs";
import { readdir as readdirAsync } from "fs/promises";
import { join } from "path";
import { Readable } from "stream";

import { Context } from "moleculer";
import _ from "lodash";

import { QueryConfig, QueryResult } from "pg";
import pgFormat from "pg-format";
import dedent from "dedent";

import { FSA } from "flux-standard-action";

export type ServiceContext = Context & {
  params: FSA;
};

import etlDir from "../../constants/etlDir";

import GeospatialDatasetIntegrator from "../../../tasks/gis-data-integration/src/data_integrators/GeospatialDatasetIntegrator";

const serviceName = "dama/data_source_integrator";

const commonDammaMetaProps = ["DAMAA", "etl_context_id"];

const EventTypes = {
  LOAD_REQUEST: `${serviceName}:LOAD_REQUEST`,
  STAGED: `${serviceName}:STAGED`,

  QA_REQUEST: `${serviceName}:QA_REQUEST`,
  QA_APPROVED: `${serviceName}:QA_APPROVED`,

  VIEW_METADATA_SUBMITTED: `${serviceName}:VIEW_METADATA_SUBMITTED`,

  READY_TO_PUBLISH: `${serviceName}:READY_TO_PUBLISH`,

  NOT_READY_TO_PUBLISH: `${serviceName}:NOT_READY_TO_PUBLISH`,

  PUBLISH: `${serviceName}:PUBLISH`,

  PUBLISH_ERROR: `${serviceName}:PUBLISH_ERROR`,

  FINAL: `${serviceName}:FINAL`,
};

const ReadyToPublishPrerequisites = [
  EventTypes.QA_APPROVED,
  EventTypes.VIEW_METADATA_SUBMITTED,
];

export default {
  name: serviceName,

  events: {
    [EventTypes.LOAD_REQUEST]: {
      context: true,
      async handler(ctx: Context) {
        console.log(JSON.stringify(ctx.params, null, 4));

        const reqEvent = <FSA>ctx.params;

        if (!reqEvent.meta.DAMAA) {
          throw new Error("LOAD_REQUEST events must be DAMAA events");
        }

        const migration_result = await this.loadDatabaseTable(ctx);

        const loadedEvent = {
          type: `${serviceName}:STAGED`,
          payload: migration_result,
          meta: {
            // @ts-ignore
            ..._.pick(reqEvent.meta, commonDammaMetaProps),
            checkpoint: true,
          },
        };

        ctx.call("dama_dispatcher.dispatch", loadedEvent);

        const qaRequestEvent = {
          type: EventTypes.QA_APPROVED,
          payload: _.pick(loadedEvent, ["tableSchema", "tableName"]),
          meta: {
            // @ts-ignore
            ..._.pick(reqEvent.meta, commonDammaMetaProps),
            checkpoint: true,
          },
        };

        ctx.call("dama_dispatcher.dispatch", qaRequestEvent);
      },
    },

    [EventTypes.QA_APPROVED]: {
      context: true,
      // params: FSAEventParam,
      async handler(ctx: Context) {
        // @ts-ignore
        if (!ctx.params.meta.DAMAA) {
          throw new Error("QA_APPROVED events must be DAMAA events");
        }

        await this.checkIfReadyToPublish(ctx);
      },
    },

    [EventTypes.VIEW_METADATA_SUBMITTED]: {
      context: true,
      // params: FSAEventParam,
      async handler(ctx: Context) {
        // @ts-ignore
        if (!ctx.params.meta.DAMAA) {
          throw new Error(
            "VIEW_METADATA_SUBMITTED events must be DAMAA events"
          );
        }

        await this.checkIfReadyToPublish(ctx);
      },
    },

    [EventTypes.PUBLISH]: {
      context: true,
      // params: FSAEventParam,
      async handler(ctx: Context) {
        // @ts-ignore
        if (!ctx.params.meta.DAMAA) {
          throw new Error("PUBLISH events must be DAMAA events");
        }

        await this.publish(ctx);
      },
    },
  },

  actions: {
    getExistingDatasetUploads: {
      visibility: "published",

      async handler() {
        const dirs = await readdirAsync(etlDir, {
          encoding: "utf8",
        });

        const ids = dirs.reduce((acc: string[], dirName: string) => {
          const workDirPath = join(etlDir, dirName);
          const path = join(workDirPath, "layerNameToId.json");

          if (existsSync(path)) {
            const id = GeospatialDatasetIntegrator.createId(workDirPath);
            acc.push(id);
          }

          return acc;
        }, []);

        return ids;
      },
    },

    uploadGeospatialDataset: {
      visibility: "published",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          meta: { filename },
          params: fileStream,
        } = ctx;

        const gdi = new GeospatialDatasetIntegrator();
        const id = await gdi.receiveDataset(
          <string>filename,
          <Readable>fileStream
        );

        return { id };
      },
    },

    getGeospatialDatasetLayerNames: {
      visibility: "published",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { id },
        } = ctx;

        const gdi = new GeospatialDatasetIntegrator(id);

        // @ts-ignore
        const layerNameToId = gdi.layerNameToId;
        const layerNames = Object.keys(layerNameToId);

        return layerNames;
      },
    },

    getTableDescriptor: {
      visibility: "published",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { id, layerName },
        } = ctx;

        const gdi = new GeospatialDatasetIntegrator(id);

        const tableDescriptor = await gdi.getLayerTableDescriptor(layerName);

        return tableDescriptor;
      },
    },

    updateTableDescriptor: {
      visibility: "published",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params,
        } = ctx;

        // @ts-ignore
        const { id } = params;

        const gdi = new GeospatialDatasetIntegrator(id);

        // @ts-ignore
        const migration_result = await gdi.persistLayerTableDescriptor(params);

        return migration_result;
      },
    },
  },

  methods: {
    loadDatabaseTable: {
      async handler(ctx: Context) {
        const {
          params: event,
          // @ts-ignore
          meta: { pgEnv },
        } = ctx;

        const {
          // @ts-ignore
          payload: { id, layerName },
        } = event;

        const gdi = new GeospatialDatasetIntegrator(id);

        const migration_result = await gdi.loadTable({ layerName, pgEnv });

        console.log(JSON.stringify({ migration_result }, null, 4));

        return migration_result;
      },
    },

    checkIfReadyToPublish: {
      async handler(ctx: Context) {
        const { params: event } = ctx;

        const {
          // @ts-ignore
          meta: { etl_context_id },
        } = event;

        const events: FSA[] = await ctx.call(
          "dama_dispatcher.queryDamaEvents",
          {
            etl_context_id,
          }
        );

        const eventTypes = new Set(events.map(({ type }) => type));

        console.log(JSON.stringify({ eventTypes: [...eventTypes] }, null, 4));

        if (ReadyToPublishPrerequisites.every((eT) => eventTypes.has(eT))) {
          const newEvent = {
            type: EventTypes.READY_TO_PUBLISH,
            payload: { etl_context_id },
            meta: (<FSA>event).meta,
          };

          process.nextTick(() =>
            ctx.call("dama_dispatcher.dispatch", newEvent)
          );

          return true;
        }

        return false;
      },
    },

    publish: {
      async handler(ctx: Context) {
        const { params: event } = ctx;

        const {
          params: { clean = false } = {},
          // @ts-ignore
          meta: { etl_context_id },
        } = event;

        const events: FSA[] = await ctx.call(
          "dama_dispatcher.queryDamaEvents",
          {
            etl_context_id,
          }
        );

        const eventByType = events.reduce((acc, damaEvent: FSA) => {
          acc[damaEvent.type] = damaEvent;
          return acc;
        }, {});

        if (!eventByType[EventTypes.READY_TO_PUBLISH]) {
          const missingPrereqs = ReadyToPublishPrerequisites.filter(
            (eT) => !eventByType[eT]
          ).map((eT) => eT.replace(/^.*:/, ""));

          if (missingPrereqs) {
            const errEvent = {
              type: EventTypes.NOT_READY_TO_PUBLISH,
              payload: {
                message: `The following PUBLISH prerequisites are not met: ${missingPrereqs}`,
              },
              // @ts-ignore
              meta: {
                ...event.meta,
                DAMAA: true,
                timestamp: new Date().toISOString(),
              },
              error: true,
            };

            return ctx.call("dama_dispatcher.dispatch", errEvent);
          }
        }

        const dataStagedEvent = eventByType[EventTypes.STAGED];

        const {
          payload: {
            tableSchema: stagedTableSchema,
            tableName: stagedTableName,
          },
        } = dataStagedEvent;

        const viewMetadataSubmittedEvent =
          eventByType[EventTypes.VIEW_METADATA_SUBMITTED];

        const {
          payload: { table_schema, table_name },
        } = viewMetadataSubmittedEvent;

        //  We accumulate the PUBLISH SQL commands into an array
        //    then execute all of them within a single transaction.
        const migration_sql: Array<string | QueryConfig> = ["BEGIN;"];

        const stagingTableIsTargetTable =
          stagedTableSchema === table_schema && stagedTableName === table_name;

        if (!stagingTableIsTargetTable) {
          if (clean) {
            migration_sql.push(
              pgFormat("DROP TABLE IF EXISTS %I.%I ;", table_schema, table_name)
            );
          }

          //  We need to rename the indexes assosciated with the table.
          //    Assumes ALL indexes prefixed by the stagedTableName (eg foo_pkey for table foo).
          //    For all indexes, the stagedTableName is replaced with the table_name.

          const indexRenameRE = new RegExp(`^${stagedTableName}`);

          const indexesQuery = dedent(
            pgFormat(
              `
              SELECT
                  indexname
                FROM pg_indexes
                WHERE (
                  ( schemaname = %L )
                  AND
                  ( tablename = %L )
                )
            `,
              stagedTableSchema,
              stagedTableName
            )
          );

          const indexesQueryResult: QueryResult = await ctx.call(
            "dama_db.query",
            indexesQuery
          );

          for (const { indexname } of indexesQueryResult.rows) {
            const newIndexName = indexname.replace(indexRenameRE, table_name);

            migration_sql.push(
              dedent(
                pgFormat(
                  "ALTER INDEX %I.%I RENAME TO %I ;",
                  stagedTableSchema,
                  indexname,
                  newIndexName
                )
              )
            );
          }

          migration_sql.push(
            pgFormat("CREATE SCHEMA IF NOT EXISTS %I ;", table_schema)
          );

          // NOTE: This will fail if there exists a table stagedTableSchema.table_name
          migration_sql.push(
            pgFormat(
              "ALTER TABLE %I.%I RENAME TO %I ;",
              stagedTableSchema,
              stagedTableName,
              table_name
            )
          );

          migration_sql.push(
            pgFormat(
              "ALTER TABLE %I.%I SET SCHEMA %I ;",
              stagedTableSchema,
              table_name,
              table_schema
            )
          );
        }

        const updateViewMetaIdx = migration_sql.length;

        const updateViewMetaSql = <QueryConfig>(
          await ctx.call(
            "dama_meta.getUpdateDataManagerViewMetadataSql",
            viewMetadataSubmittedEvent
          )
        );

        migration_sql.push(updateViewMetaSql);

        migration_sql.push("COMMIT;");

        try {
          const migration_result = <QueryResult[]>(
            await ctx.call("dama_db.query", migration_sql)
          );

          // We need the data_manager.views id
          const {
            rows: [{ id: data_manager_view_id }],
          } = migration_result[updateViewMetaIdx];

          const type = EventTypes.FINAL;
          const payload = {
            data_manager_view_id,
            migration_sql,
            migration_result,
          };

          const meta = {
            // @ts-ignore
            ...event.meta,
            timestamp: new Date().toISOString(),
          };

          const finalEvent = { type, payload, meta };

          await ctx.call("dama_dispatcher.dispatch", finalEvent);

          return finalEvent;
        } catch (err) {
          console.error(err);

          const errEvent = {
            type: EventTypes.PUBLISH_ERROR,
            payload: { message: err.message },
            meta: {
              // @ts-ignore
              ...event.meta,
              timestamp: new Date().toISOString(),
            },
          };

          return ctx.call("dama_dispatcher.dispatch", errEvent);
        }
      },
    },
  },
};
