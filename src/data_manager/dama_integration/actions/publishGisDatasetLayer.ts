import { Context } from "moleculer";

import { PoolClient, QueryConfig, QueryResult } from "pg";
import pgFormat from "pg-format";
import dedent from "dedent";
import _ from "lodash";

import { FSA } from "flux-standard-action";

import EventTypes from "../constants/EventTypes";

export const ReadyToPublishPrerequisites = [
  EventTypes.QA_APPROVED,
  EventTypes.VIEW_METADATA_SUBMITTED,
];

async function checkIfReadyToPublish(ctx: Context, events: FSA[]) {
  const {
    // @ts-ignore
    params: { etl_context_id },
  } = ctx;

  const eventTypes = events.map(({ type }) => type);

  const unmetPreReqs = _.difference(
    ReadyToPublishPrerequisites,
    eventTypes
  ).map((e) => e.replace(/.*:/, ""));

  if (unmetPreReqs.length) {
    const message = `The following PUBLISH prerequisites are not met: ${unmetPreReqs}`;

    const errEvent = {
      type: EventTypes.NOT_READY_TO_PUBLISH,
      payload: {
        message,
      },
      meta: {
        etl_context_id,
        timestamp: new Date().toISOString(),
      },
      error: true,
    };

    await ctx.call("dama_dispatcher.dispatch", errEvent);

    console.error("errEvent:", errEvent);

    throw new Error(message);
  }
}

async function getInsertViewMetadataSql(
  ctx: Context,
  viewMetadataSubmittedEvent: FSA
) {
  const insertViewMetaSql = <QueryConfig>(
    await ctx.call(
      "dama/metadata.getInsertDataManagerViewMetadataSql",
      viewMetadataSubmittedEvent
    )
  );

  return insertViewMetaSql;
}

async function generatePublishSql(
  ctx: Context,
  dataStagedEvent: FSA,
  table_schema: string,
  table_name: string
) {
  const {
    // @ts-ignore
    payload: { tableSchema: stagedTableSchema, tableName: stagedTableName },
  } = dataStagedEvent;

  const publishSql: Array<string | QueryConfig> = ["BEGIN;"];

  const stagingTableIsTargetTable =
    stagedTableSchema === table_schema && stagedTableName === table_name;

  if (!stagingTableIsTargetTable) {
    // //   For now, disable clean (DROP TABLE IF EXISTS) foot nuke.
    // //     TODO: Have a whitelist for data sources that will allow DROPs.
    //
    // if (clean) {
    // publishSql.push(
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

      publishSql.push(
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

    publishSql.push(pgFormat("CREATE SCHEMA IF NOT EXISTS %I ;", table_schema));

    // NOTE: This will fail if there exists a table stagedTableSchema.table_name
    publishSql.push(
      pgFormat(
        "ALTER TABLE %I.%I RENAME TO %I ;",
        stagedTableSchema,
        stagedTableName,
        table_name
      )
    );

    publishSql.push(
      pgFormat(
        "ALTER TABLE %I.%I SET SCHEMA %I ;",
        stagedTableSchema,
        table_name,
        table_schema
      )
    );
  }

  return publishSql;
}

export default async function publish(ctx: Context) {
  // throw new Error("publish TEST ERROR");
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

  await checkIfReadyToPublish(ctx, events);

  const dbConnection: PoolClient = await ctx.call("dama_db.getDbConnection");
  const sqlLog: any[] = [];
  const resLog: QueryResult[] = [];

  try {
    let res: QueryResult;

    sqlLog.push("BEGIN ;");
    res = await dbConnection.query("BEGIN ;");
    resLog.push(res);

    const viewMetadataSubmittedEvent =
      eventByType[EventTypes.VIEW_METADATA_SUBMITTED];

    const insertViewMetaSql = await getInsertViewMetadataSql(
      ctx,
      viewMetadataSubmittedEvent
    );

    sqlLog.push(insertViewMetaSql);
    res = await dbConnection.query(insertViewMetaSql);

    const {
      rows: [viewMetadata],
    } = res;

    const {
      id: dama_view_id,
      table_schema: origTableSchema,
      table_name: origTableName,
    } = viewMetadata;

    const table_schema = origTableSchema || "gis_datasets";
    let table_name = origTableName;

    if (!origTableName) {
      const dataSrcName = viewMetadataSubmittedEvent.payload.data_source_name;
      const normalizedDataSrcName = _.snakeCase(
        dataSrcName.replace(/^0-9a-z/gi, "_").replace(/_{1,}/, "_")
      );
      table_name = `${normalizedDataSrcName}__v${dama_view_id}`;
    }

    if (origTableSchema !== table_schema || origTableName !== table_name) {
      const updateViewMetaSql = dedent(
        `
          UPDATE data_manager.views
            SET
              table_schema  = $1,
              table_name    = $2,
              data_table    = $3
            WHERE id = $4
        `
      );

      const data_table = pgFormat("%I.%I", table_schema, table_name);

      const q = {
        text: updateViewMetaSql,
        values: [table_schema, table_name, data_table, dama_view_id],
      };

      sqlLog.push(q);
      res = await dbConnection.query(q);
      resLog.push(res);
    }

    const dataStagedEvent = eventByType[EventTypes.LAYER_DATA_STAGED];

    const publishSql = await generatePublishSql(
      ctx,
      dataStagedEvent,
      table_schema,
      table_name
    );

    for (const cmd of publishSql) {
      sqlLog.push(cmd);
      res = await dbConnection.query(cmd);
      resLog.push(res);
    }

    // We need the data_manager.views id
    dbConnection.query("COMMIT;");
    dbConnection.release();

    console.log(`PUBLISHED: ${table_schema}.${table_name}`);

    const finalEvent = {
      type: EventTypes.FINAL,
      payload: {
        data_manager_view_id: -1,
        publishSql: sqlLog,
        publishCmdResults: resLog,
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
      payload: {
        message: err.message,
        successfulPublishSql: sqlLog,
        successfulPublishCmdResults: resLog,
      },
      meta: {
        etl_context_id,
        user_id,
        timestamp: new Date().toISOString(),
      },
    };

    await ctx.call("dama_dispatcher.dispatch", errEvent);

    await dbConnection.query("ROLLBACK;");

    throw err;
  }
}
