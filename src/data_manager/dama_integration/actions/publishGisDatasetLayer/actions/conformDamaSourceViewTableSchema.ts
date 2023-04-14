import _ from "lodash";
import pgFormat from "pg-format";
import dedent from "dedent";

import { Context as MoleculerContext } from "moleculer";

export default async function conformDamaSourceViewTableSchema(
  ctx: MoleculerContext
) {
  const {
    params: {
      // @ts-ignore
      newDamaView: { source_id: damaSourceId, view_id: damaViewId },
    },
  } = ctx;

  const tableSchemasSummarySql = dedent(`
    SELECT
        table_schemas_summary
      FROM _data_manager_admin.dama_source_distinct_view_table_schemas
      WHERE ( source_id = $1 )
  `);

  const { rows: tableSchemasSummaryResult } = await ctx.call("dama_db.query", {
    text: tableSchemasSummarySql,
    values: [damaSourceId],
  });

  if (!tableSchemasSummaryResult.length) {
    throw new Error(`Invalid DaMa SourceID: ${damaSourceId}`);
  }

  const [{ table_schemas_summary: tableSchemasSummary }] =
    tableSchemasSummaryResult;

  if (!Array.isArray(tableSchemasSummary) || tableSchemasSummary.length === 0) {
    throw new Error(`No tableSchemasSummary for DaMaSource: ${damaSourceId}`);
  }

  if (tableSchemasSummary.length === 1) {
    // Everything is already consistent
    return;
  }

  const { table_schema: thisViewTableSchema } = tableSchemasSummary.find(
    ({ view_ids }) => view_ids.includes(damaViewId)
  );

  const otherViewsSchemaSummaries = tableSchemasSummary.filter(
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
  } = await ctx.call("dama_db.query", {
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
      WHERE ( view_id = $1 )
  `);

  const {
    rows: [{ table_schema, table_name }],
  } = await ctx.call("dama_db.query", {
    text: viewDataTableSql,
    values: [damaViewId],
  });

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

    await ctx.call("dama_db.query", addColSql);
  }

  return schemasDiff;
}
