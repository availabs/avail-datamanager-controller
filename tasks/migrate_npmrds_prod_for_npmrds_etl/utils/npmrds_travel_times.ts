import { readFileSync } from "fs";
import { join } from "path";

import _ from "lodash";
import pgFormat from "pg-format";
import { DateTime } from "luxon";

import dama_db from "../../../src/data_manager/dama_db";

import { TableInfo, InheritanceTree, state_re } from "../domain";

const query_inheritance_tree_sql_fpath = join(
  __dirname,
  "../sql/queryNpmrdsAuthoritativePartitionTree.sql"
);

export function parseLeafTableInfo(table_info: TableInfo) {
  const { table_schema, table_name } = table_info;

  if (!state_re.test(table_schema)) {
    throw new Error(
      `Invalid table_schema: ${table_schema}. Expected a state abbreviation.`
    );
  }

  const data_table_name_re = /^npmrds_y\d{4}m\d{2}$/;

  if (!data_table_name_re.test(table_name)) {
    const table_full_name = pgFormat("%I.%I", table_schema, table_name);
    throw new Error(`Invalid table_name: ${table_full_name}`);
  }

  const year = table_name.slice(8, 12);
  const month = table_name.slice(13);

  const start_date = `${year}-${month}-01`;
  const start_date_time = DateTime.fromISO(start_date);

  const end_date_time = start_date_time.plus({ month: 1 });
  const end_date = end_date_time.toISODate();

  return { state: table_schema, start_date, end_date };
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
