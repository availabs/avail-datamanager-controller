import { readFileSync } from "fs";
import { join } from "path";

import _ from "lodash";
import pgFormat from "pg-format";

import dama_db from "../../../src/data_manager/dama_db";

import { TableInfo, InheritanceTree, state_re } from "../domain";

const query_inheritance_tree_sql_fpath = join(
  __dirname,
  "../sql/queryTmcIdentificationAuthoritativePartitionTree.sql"
);

export type InheritanceTreesByYear = Record<string, InheritanceTree>;

export const leaf_table_re = /^tmc_identification_\d{4}_v\d{8}t\d{6}$/;

export async function getTmcIdentificationInheritanceTreesByYear(): Promise<InheritanceTreesByYear> {
  const sql = readFileSync(query_inheritance_tree_sql_fpath, {
    encoding: "utf8",
  });

  const { rows } = await dama_db.query(sql);

  const table_re = /^tmc_identification_\d{4}/;

  const root_tables_by_year: Record<string, InheritanceTree> = {};

  for (const row of rows) {
    const { table_schema, table_name } = row;

    if (!table_re.test(table_name)) {
      const table_full_name = pgFormat("%I.%I", table_schema, table_name);
      throw new Error(`Invalid table name: ${table_full_name}`);
    }

    const [, , year] = table_name.split("_");

    root_tables_by_year[year] = root_tables_by_year[year] || [];
    root_tables_by_year[year].push(row);
  }

  return root_tables_by_year;
}

export function parseLeafTableInfo(table_info: TableInfo) {
  const { table_schema, table_name } = table_info;

  if (!state_re.test(table_schema)) {
    throw new Error(
      `Invalid table_schema: ${table_schema}. Expected a state abbreviation.`
    );
  }

  if (!leaf_table_re.test(table_name)) {
    throw new Error(
      `Invalid table_name: ${table_name}. Expected to match /^tmc_identification_\\d{4}_v\\d{8}t\\d{6}$/`
    );
  }

  const data_table_name_re = /^tmc_identification_\d{4}_*/;

  if (!data_table_name_re.test(table_name)) {
    const table_full_name = pgFormat("%I.%I", table_schema, table_name);
    throw new Error(`Invalid table_name: ${table_full_name}`);
  }

  const [, , year, version] = table_name.split("_");

  const start_date = `${year}-01-01`;
  const end_date = `${year}-12-31`;

  return { state: table_schema, year, start_date, end_date, version };
}

export function getRootTable(inheritance_tree: InheritanceTree): {
  root_table_schema: string;
  root_table_name: string;
} {
  const roots = inheritance_tree.filter(({ depth }) => depth === 0);

  if (roots.length !== 1) {
    console.error(JSON.stringify({ roots }, null, 4));
  }

  const [{ table_schema: root_table_schema, table_name: root_table_name }] =
    roots;

  return { root_table_schema, root_table_name };
}

export function getLeafTables(inheritance_tree: InheritanceTree): TableInfo[] {
  const parent_tables_set = new Set(
    inheritance_tree
      .map(({ parent_table_schema, parent_table_name }) =>
        parent_table_name
          ? pgFormat("%I.%I", parent_table_schema, parent_table_name)
          : null
      )
      .filter(Boolean)
  );

  const leaf_tables = inheritance_tree.filter(
    ({ table_schema, table_name }) =>
      !parent_tables_set.has(pgFormat("%I.%I", table_schema, table_name))
  );

  return leaf_tables;
}
