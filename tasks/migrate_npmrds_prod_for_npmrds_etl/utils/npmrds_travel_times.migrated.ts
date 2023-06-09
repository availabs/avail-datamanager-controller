import { readFileSync } from "fs";
import { join } from "path";

import _ from "lodash";
import pgFormat from "pg-format";
import { DateTime } from "luxon";

import dama_db from "../../../src/data_manager/dama_db";

import {
  NpmrdsState,
  NpmrdsDatabaseSchemas,
} from "../../../src/data_types/npmrds/domain";
import { TableInfo, InheritanceTree, state_re } from "../domain";

const query_inheritance_tree_sql_fpath = join(
  __dirname,
  "../sql/queryNpmrdsAuthoritativePartitionTree.sql"
);

const leaf_table_name_re = /npmrdsx?_[a-z]{2}_from_\d{8}_to_\d{8}_v\d{8}t\d{6}/;

export function parseLeafTableInfo(table_info: TableInfo) {
  const { table_schema, table_name } = table_info;

  if (!state_re.test(table_schema)) {
    throw new Error(
      `Invalid table_schema: ${table_schema}. Expected a state abbreviation.`
    );
  }

  if (!leaf_table_name_re.test(table_name)) {
    const table_full_name = pgFormat("%I.%I", table_schema, table_name);
    throw new Error(`Invalid table_name: ${table_full_name}`);
  }

  const [, state, , start, , end] = table_name.split("_");

  if (!NpmrdsState[state]) {
    throw new Error(`Invalid state: ${state}`);
  }

  const format_str = "yyyyMMdd";

  const start_date = DateTime.fromFormat(start, format_str).toISODate();
  const end_date = DateTime.fromFormat(end, format_str).toISODate();

  if (!start_date) {
    throw new Error(`Invalid start: ${start}`);
  }

  if (!end_date) {
    throw new Error(`Invalid end: ${end}`);
  }

  return { state, start_date, end_date };
}

export async function getNpmrdsTablesInheritanceTree(): Promise<InheritanceTree> {
  const sql = readFileSync(query_inheritance_tree_sql_fpath, {
    encoding: "utf8",
  });

  const { rows } = await dama_db.query(sql);

  return rows;
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

// NOTE: After detaching, some npmrds_travel_times schema tables have no children,
//       so they are actually leaves but omitted from the return value of this function.
export function getLeafTables(inheritance_tree: InheritanceTree): TableInfo[] {
  const leaf_tables = inheritance_tree.filter(
    ({ table_schema }) =>
      table_schema === NpmrdsDatabaseSchemas.NpmrdsTravelTimesImports
  );

  return leaf_tables;
}
