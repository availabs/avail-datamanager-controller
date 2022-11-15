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

async function initializeDamaSourceMetadataUsingViews({
  execQuery,
  damaSourceId,
}: any) {
  const dataSrcMetaSql = dedent(`
    SELECT
        metadata
      FROM data_manager.sources
      WHERE ( id = $1 )
  `);

  let rows: QueryResult["rows"];

  rows = (
    await execQuery({
      text: dataSrcMetaSql,
      values: [damaSourceId],
    })
  ).rows;

  if (rows.length === 0) {
    throw new Error(`Invalid DaMa SourceID: ${damaSourceId}`);
  }

  const [{ metadata }] = rows;

  // Already has metadata. NoOp.
  if (metadata) {
    return metadata;
  }

  const consistentDamaViewMetaSql = dedent(`
    SELECT
        views_metadata_summary
      FROM _data_manager_admin.dama_source_distinct_view_metadata
      WHERE (
        ( source_id = $1 )
      )
  `);

  rows = (
    await execQuery({
      text: consistentDamaViewMetaSql,
      values: [damaSourceId],
    })
  ).rows;

  if (!rows.length) {
    throw new Error(`No views for DaMaSource ${damaSourceId}`);
  }

  const [{ views_metadata_summary }] = rows;

  if (views_metadata_summary.length > 1) {
    throw new Error(
      `DaMaSource ${damaSourceId} Views metadata are inconsistent. Cannot auto-initialize the source metadata.`
    );
  }

  const [
    {
      view_ids: [view_id],
      view_metadata,
    },
  ] = views_metadata_summary;

  const initDataSrcMetadataSql = dedent(`
    CALL _data_manager_admin.initialize_dama_src_metadata_using_view( $1 );
  `);

  await execQuery({ text: initDataSrcMetadataSql, values: [view_id] });

  return view_metadata;
}

async function conformDamaSourceViewTableSchema({
  execQuery,
  damaViewId,
}: any) {
  let rows: QueryResult["rows"];

  const damaViewSourceIdSql = dedent(`
    SELECT
        source_id
      FROM data_manager.views
      WHERE ( id = $1 )
  `);

  rows = (
    await execQuery({
      text: damaViewSourceIdSql,
      values: [damaViewId],
    })
  ).rows;

  if (rows.length === 0) {
    throw new Error(`Invalid DaMa ViewID: ${damaViewId}`);
  }

  const [{ source_id: damaSourceId }] = rows;

  const dataSrcMetaSql = dedent(`
    SELECT
        table_schemas_summary
      FROM _data_manager_admin.dama_source_distinct_view_table_schemas
      WHERE ( source_id = $1 )
  `);

  rows = (
    await execQuery({
      text: dataSrcMetaSql,
      values: [damaSourceId],
    })
  ).rows;

  if (rows.length === 0) {
    throw new Error(`Invalid DaMa SourceID: ${damaSourceId}`);
  }

  const [{ table_schemas_summary }] = rows;

  if (
    !Array.isArray(table_schemas_summary) ||
    table_schemas_summary.length === 0
  ) {
    throw new Error(`No table_schemas_summary for DaMaSource: ${damaSourceId}`);
  }

  if (table_schemas_summary.length === 1) {
    // Everything is already consistent
    return;
  }

  const { table_schema: thisViewTableSchema } = table_schemas_summary.find(
    ({ view_ids }) => view_ids.includes(damaViewId)
  );

  const otherViewsSchemaSummaries = table_schemas_summary.filter(
    ({ view_ids }) => !_.isEqual(view_ids, [damaViewId])
  );

  if (otherViewsSchemaSummaries.length > 1) {
    throw new Error(
      `The other DaMaSource Views table schemas are inconsistent. Cannot auto-conform view ${damaViewId}.`
    );
  }

  const [{ view_ids: otherViewIds, table_schema: otherViewsTableSchema }] =
    otherViewsSchemaSummaries;

  const otherSchemasConsistentWithDataSourceMetadataSql = dedent(`
    SELECT NOT EXISTS (
      SELECT
          1
        FROM _data_manager_admin.dama_views_metadata_conformity
        WHERE (
          ( view_id = ANY($1) )
          AND
          (
            ( source_metadata_only IS NOT NULL )
            OR
            ( view_metadata_only IS NOT NULL )
          )
        )
    ) AS all_other_schemas_consistent;
  `);

  const {
    rows: [{ all_other_schemas_consistent }],
  } = await execQuery({
    text: otherSchemasConsistentWithDataSourceMetadataSql,
    values: [otherViewIds],
  });

  if (!all_other_schemas_consistent) {
    throw new Error(
      "The other DaMa Views are inconsistent with the DaMa Source metadata."
    );
  }

  const allColumns = _.union(
    Object.keys(thisViewTableSchema),
    Object.keys(otherViewsTableSchema)
  );

  const schemasDiff = allColumns.reduce((acc, col) => {
    const thisViewColType = thisViewTableSchema[col] || null;
    const otherViewsColType = otherViewsTableSchema[col] || null;

    if (thisViewColType !== otherViewsColType) {
      acc[col] = { thisView: thisViewColType, otherViews: otherViewsColType };
    }

    return acc;
  }, {});

  const diffCols = Object.keys(schemasDiff);

  const diffOnlyOmittedCols = diffCols.every(
    (col) => schemasDiff[col].thisView === null
  );

  console.log(JSON.stringify({ schemasDiff, diffOnlyOmittedCols }, null, 4));

  if (!diffOnlyOmittedCols) {
    throw new Error(
      `conformDamaSourceViewTableSchema currently only supports adding columns to a View's data table. The the schema diff requires more: ${JSON.stringify(
        schemasDiff
      )}`
    );
  }

  const viewDataTableSql = dedent(`
    SELECT
        table_schema,
        table_name
      FROM data_manager.views
      WHERE ( id = $1 )
  `);

  const {
    rows: [{ table_schema, table_name }],
  } = await execQuery({ text: viewDataTableSql, values: [damaViewId] });

  for (const col of diffCols) {
    const addColSql = dedent(
      pgFormat(
        `
          ALTER TABLE %I.%I
            ADD COLUMN %I ${schemasDiff[col].otherViews}
          ;
        `,
        table_schema,
        table_name,
        col
      )
    );

    console.log(addColSql);

    await execQuery(addColSql);
  }

  return schemasDiff;
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

  const execQuery = async (q: any): Promise<QueryResult> => {
    sqlLog.push(q);
    const res = await dbConnection.query(q);
    resLog.push(res);

    return res;
  };

  try {
    await execQuery("BEGIN ;");

    const viewMetadataSubmittedEvent =
      eventByType[EventTypes.VIEW_METADATA_SUBMITTED];

    const insertViewMetaSql = await getInsertViewMetadataSql(
      ctx,
      viewMetadataSubmittedEvent
    );

    const {
      rows: [viewMetadata],
    } = await execQuery(insertViewMetaSql);

    const {
      id: damaViewId,
      source_id: damaSourceId,
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
      table_name = `${normalizedDataSrcName}__v${damaViewId}`;
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
        values: [table_schema, table_name, data_table, damaViewId],
      };

      await execQuery(q);
    }

    const dataStagedEvent = eventByType[EventTypes.LAYER_DATA_STAGED];

    const publishSql = await generatePublishSql(
      ctx,
      dataStagedEvent,
      table_schema,
      table_name
    );

    for (const q of publishSql) {
      await execQuery(q);
    }

    let initializeDamaSourceMetadataWarning: string | undefined;

    try {
      await initializeDamaSourceMetadataUsingViews({ execQuery, damaSourceId });
    } catch (err) {
      console.error(err);
      initializeDamaSourceMetadataWarning = err.message;
    }

    let conformDamaSourceViewTableSchemaWarning: string | undefined;

    try {
      await conformDamaSourceViewTableSchema({ execQuery, damaViewId });
    } catch (err) {
      console.error(err);
      conformDamaSourceViewTableSchemaWarning = err.message;
    }

    await execQuery("COMMIT");
    dbConnection.release();

    console.log(`PUBLISHED: ${table_schema}.${table_name}`);

    const publishSqlLog = sqlLog.map((q, i) => ({
      query: q,
      result: resLog[i],
    }));

    const finalEvent = {
      type: EventTypes.FINAL,
      payload: {
        damaSourceId,
        damaViewId,
        publishSqlLog,
        initializeDamaSourceMetadataWarning,
        conformDamaSourceViewTableSchemaWarning,
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
