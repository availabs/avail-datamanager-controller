import dedent from "dedent";
import _ from "lodash";
import pgFormat from "pg-format";

import { Context } from "moleculer";

import { NodePgQueryConfig } from "../../dama_db/postgres/PostgreSQL";

export type Query = string | NodePgQueryConfig;

function warnAboutAdditionalMetadataProps(
  metaProps: string[],
  insertCols: string[]
) {
  const xtraMetaProps = _.difference(metaProps, insertCols).filter(
    (c) => c !== "source_dependencies_names"
  );

  if (metaProps.includes("source_dependencies")) {
    console.warn(
      'DamaSource source_dependencies cannot be specified in the metadata. Use "source_dependencies_names".'
    );
  }

  if (xtraMetaProps.length) {
    console.warn(
      `The following DamaSource properties cannot be inserted into data_manager.sources: ${xtraMetaProps}.`
    );
  }
}

export type LoadDataSourcesQueries = {
  name: string;
  existsQuery: string | NodePgQueryConfig;
  insertQuery: string | NodePgQueryConfig;
  allSourceDependencyNames: null | string[];
  existingSourceDependencyNamesQuery: null | NodePgQueryConfig;
  updateSourceDependenciesQuery: null | NodePgQueryConfig;
};

export type ToposortedLoadDataSourcesQueries = LoadDataSourcesQueries[];

export default async function generateToposortedLoadDataSourcesQueries(
  ctx: Context
) {
  const {
    // @ts-ignore
    params: { toposortedDataSourcesMetadata },
  } = ctx;

  const tableDescription = await ctx.call(
    "dama_db.describeTable",
    {
      tableSchema: "data_manager",
      tableName: "sources",
    },
    { parentCtx: ctx }
  );

  // @ts-ignore
  const tableCols = Object.keys(tableDescription);

  const insertableColumns = tableCols.filter(
    (c) => c !== "source_dependencies"
  );

  const stmts: ToposortedLoadDataSourcesQueries = [];

  for (const dataSrcMetadata of toposortedDataSourcesMetadata) {
    const {
      name,
      source_dependencies_names,
    }: {
      name: string;
      source_dependencies_names: null | string[] | string[][];
    } = dataSrcMetadata;

    const existsQuery = dedent(
      pgFormat(
        `
          SELECT EXISTS (
            SELECT
                1
              FROM data_manager.sources
              WHERE ( name = %L )
          ) AS data_source_exists ;
        `,
        name
      )
    );

    const metaProps = Object.keys(dataSrcMetadata);
    const insertCols = _.intersection(metaProps, insertableColumns);
    const insertValues: any[] = [];

    warnAboutAdditionalMetadataProps(metaProps, insertCols);

    const insertParams: string[] = [];

    for (const col of insertCols) {
      insertValues.push(dataSrcMetadata[col]);
      insertParams.push(`$${insertValues.length}`);
    }

    const colFormatStrs = insertCols.map(() => "%I");

    const insertText = dedent(
      pgFormat(
        `
          INSERT INTO data_manager.sources( ${colFormatStrs.join(", ")} )
            VALUES ( ${insertParams.join(", ")} )
            ON CONFLICT DO NOTHING
            RETURNING * ;
        `,
        ...insertCols
      )
    );

    const insertQuery = {
      text: insertText,
      values: insertValues,
    };

    //  Here we UPDATE the source_dependencies column.
    //
    //    This MUST be done after the INSERT because some DamaSources,
    //      such as the NpmrdsAuthoritativeTravelTimesDb,
    //      use table inheritance/partitioning and therefore
    //      are represented as a (recursive) Tree data structure.
    //
    //        * at the leaves, the source_dependencies are NpmrdsTravelTimesExportDb
    //        * at the internal nodes, the source_dependencies are NpmrdsAuthoritativeTravelTimesDb
    //
    let allSourceDependencyNames: null | string[] = null;
    let existingSourceDependencyNamesQuery: null | NodePgQueryConfig = null;

    let updateSourceDependenciesQuery: null | NodePgQueryConfig = null;

    if (
      Array.isArray(source_dependencies_names) &&
      source_dependencies_names.length
    ) {
      const updateValues: any = [];

      let sourceDependeciesSubquery: string;

      //  source_dependencies arrays can be 1 or 2 dimensional arrays of source_ids
      //    1 dimensional arrays of source_ids represent (a AND b)
      //    2 dimensional arrays represent ((a AND b) OR (c AND d))
      if (Array.isArray(source_dependencies_names[0])) {
        const nestedArraySubqueries: string[] = [];

        for (const sourceDepNamesArr of source_dependencies_names) {
          updateValues.push(sourceDepNamesArr);

          nestedArraySubqueries.push(`
              (
                SELECT
                    array_agg(source_id ORDER BY source_id) AS deps
                  FROM data_manager.sources
                  WHERE ( name = ANY( $${updateValues.length} ) )
              )`);
        }

        sourceDependeciesSubquery = `
            ARRAY[
              ${nestedArraySubqueries.join(",\n")}
            ]
        `;
      } else {
        updateValues.push(source_dependencies_names);
        sourceDependeciesSubquery = `
            SELECT
                array_agg(source_id ORDER BY source_id) AS deps
              FROM data_manager.sources
              WHERE ( name = ANY( $${updateValues.length} ) )
        `;
      }

      updateValues.push(name);

      const updateText = dedent(`
        UPDATE data_manager.sources
          SET source_dependencies = (
            ${sourceDependeciesSubquery}
          )
          WHERE ( name = $${updateValues.length} )
          RETURNING *
        ;
      `);

      updateSourceDependenciesQuery = {
        text: updateText,
        values: updateValues,
      };

      allSourceDependencyNames = _(source_dependencies_names)
        // @ts-ignore
        .flattenDeep()
        .uniq()
        .value();

      const existingSourceDependencyNamesText = dedent(`
        SELECT
            array_agg(name) AS existing_source_dependency_names
          FROM data_manager.sources
          WHERE ( name = ANY($1) )
      `);

      const existingSourceDependencyNamesValues = [allSourceDependencyNames];

      existingSourceDependencyNamesQuery = {
        text: existingSourceDependencyNamesText,
        values: existingSourceDependencyNamesValues,
      };
    }

    stmts.push({
      name,
      existsQuery,
      insertQuery,
      allSourceDependencyNames,
      existingSourceDependencyNamesQuery,
      updateSourceDependenciesQuery,
    });
  }

  return stmts;
}
