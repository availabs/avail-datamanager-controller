import dedent from "dedent";
import pgFormat from "pg-format";

import { Context as MoleculerContext } from "moleculer";

import { NodePgQueryResult } from "../../../../dama_db/postgres/PostgreSQL";

import GisDatasetIntegrationEventTypes from "../../../constants/EventTypes";

export default async function publishStagedDataset(ctx: MoleculerContext) {
  const { params } = ctx;

  const {
    eventsByType: {
      [GisDatasetIntegrationEventTypes.LAYER_DATA_STAGED]: dataStagedEvents,
    },
    // @ts-ignore
    newDamaView: { table_schema: tableSchema, table_name: tableName },
  } = params;

  if (dataStagedEvents.length !== 1) {
    throw new Error("There must be a single LAYER_DATA_STAGED event.");
  }

  const [dataStagedEvent] = dataStagedEvents;

  const {
    // @ts-ignore
    payload: { tableSchema: stagedTableSchema, tableName: stagedTableName },
  } = dataStagedEvent;

  const stagingTableIsTargetTable =
    stagedTableSchema === tableSchema && stagedTableName === tableName;

  if (stagingTableIsTargetTable) {
    return;
  }

  // For all indexes, the stagedTableName is replaced with the table_name.

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

  const indexesQueryResult: NodePgQueryResult = await ctx.call(
    "dama_db.query",
    indexesQuery
  );

  for (const { indexname } of indexesQueryResult.rows) {
    const newIndexName = indexname.replace(indexRenameRE, tableName);

    const q = dedent(
      pgFormat(
        "ALTER INDEX %I.%I RENAME TO %I ;",
        stagedTableSchema,
        indexname,
        newIndexName
      )
    );

    await ctx.call("dama_db.query", q);
  }

  await ctx.call(
    "dama_db.query",
    pgFormat("CREATE SCHEMA IF NOT EXISTS %I ;", tableSchema)
  );

  // NOTE: This will fail if there exists a table stagedTableSchema.table_name
  await ctx.call(
    "dama_db.query",
    pgFormat(
      "ALTER TABLE %I.%I RENAME TO %I ;",
      stagedTableSchema,
      stagedTableName,
      tableName
    )
  );

  await ctx.call(
    "dama_db.query",
    pgFormat(
      "ALTER TABLE %I.%I SET SCHEMA %I ;",
      stagedTableSchema,
      tableName,
      tableSchema
    )
  );
}
