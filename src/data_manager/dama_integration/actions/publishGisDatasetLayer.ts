import { Context } from "moleculer";

import { QueryConfig, QueryResult } from "pg";
import pgFormat from "pg-format";
import dedent from "dedent";
import _ from "lodash";

import { FSA } from "flux-standard-action";

import EventTypes from "../constants/EventTypes";
import ReadyToPublishPrerequisites from "../constants/ReadyToPublishPrerequisites";

async function generateMigrationSql(
  ctx: Context,
  viewMetadataSubmittedEvent: FSA,
  dataStagedEvent: FSA
) {
  const {
    // @ts-ignore
    payload: { table_schema, table_name },
  } = viewMetadataSubmittedEvent;

  const {
    // @ts-ignore
    payload: { tableSchema: stagedTableSchema, tableName: stagedTableName },
  } = dataStagedEvent;

  const migrationSql: Array<string | QueryConfig> = ["BEGIN;"];

  const stagingTableIsTargetTable =
    stagedTableSchema === table_schema && stagedTableName === table_name;

  if (!stagingTableIsTargetTable) {
    // //   For now, disable clean (DROP TABLE IF EXISTS) foot nuke.
    // //     TODO: Have a whitelist for data sources that will allow DROPs.
    //
    // if (clean) {
    // migrationSql.push(
    // pgFormat("DROP TABLE IF EXISTS %I.%I ;", table_schema, table_name)
    // );
    // }

    //  We need to rename the indexes associated with the table.
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

      migrationSql.push(
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

    migrationSql.push(
      pgFormat("CREATE SCHEMA IF NOT EXISTS %I ;", table_schema)
    );

    // NOTE: This will fail if there exists a table stagedTableSchema.table_name
    migrationSql.push(
      pgFormat(
        "ALTER TABLE %I.%I RENAME TO %I ;",
        stagedTableSchema,
        stagedTableName,
        table_name
      )
    );

    migrationSql.push(
      pgFormat(
        "ALTER TABLE %I.%I SET SCHEMA %I ;",
        stagedTableSchema,
        table_name,
        table_schema
      )
    );
  }

  const updateViewMetaIdx = migrationSql.length;

  const updateViewMetaSql = <QueryConfig>(
    await ctx.call(
      "dama/metadata.getUpdateDataManagerViewMetadataSql",
      viewMetadataSubmittedEvent
    )
  );

  migrationSql.push(updateViewMetaSql);

  migrationSql.push("COMMIT;");

  return { migrationSql, updateViewMetaIdx };
}

export default async function publish(ctx: Context) {
  const {
    // @ts-ignore
    params: { etl_context_id, user_id },
  } = ctx;

  if (!(etl_context_id && user_id)) {
    throw new Error("The etl_context_id and user_id parameters are required.");
  }

  const events: FSA[] = await ctx.call("dama_dispatcher.queryDamaEvents", {
    etl_context_id,
  });

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
        meta: {
          etl_context_id,
          timestamp: new Date().toISOString(),
        },
        error: true,
      };

      return ctx.call("dama_dispatcher.dispatch", errEvent);
    }
  }

  const dataStagedEvent = eventByType[EventTypes.LAYER_DATA_STAGED];

  const viewMetadataSubmittedEvent =
    eventByType[EventTypes.VIEW_METADATA_SUBMITTED];

  const { migrationSql, updateViewMetaIdx } = await generateMigrationSql(
    ctx,
    viewMetadataSubmittedEvent,
    dataStagedEvent
  );

  try {
    const migration_result = // @ts-ignore
      (await ctx.call("dama_db.query", migrationSql)).map(
        (result: QueryResult) => _.omit(result, "_types")
      );

    // We need the data_manager.views id
    const {
      rows: [{ id: data_manager_view_id }],
    } = migration_result[updateViewMetaIdx];

    const finalEvent = {
      type: EventTypes.FINAL,
      payload: {
        data_manager_view_id,
        migration_sql: migrationSql,
        migration_result,
      },
      meta: {
        etl_context_id,
        user_id,
        timestamp: new Date().toISOString(),
      },
    };

    await ctx.call("dama_dispatcher.dispatch", finalEvent);

    return finalEvent;
  } catch (err) {
    console.error(err);

    const errEvent = {
      type: EventTypes.PUBLISH_ERROR,
      payload: { message: err.message },
      meta: {
        etl_context_id,
        user_id,
        timestamp: new Date().toISOString(),
      },
    };

    return ctx.call("dama_dispatcher.dispatch", errEvent);
  }
}
