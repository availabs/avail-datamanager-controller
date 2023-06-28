import dedent from "dedent";
import pgFormat from "pg-format";

import dama_db from "data_manager/dama_db";

import { NpmrdsState } from "data_types/npmrds/domain";

import create_root_npmrds_travel_times_table from "./create_root_npmrds_travel_times_table";

export default async function main(state: NpmrdsState) {
  if (!NpmrdsState[state]) {
    throw new Error(`Invalid state: ${state}`);
  }

  const { table_schema: parent_table_schema, table_name: parent_table_name } =
    await create_root_npmrds_travel_times_table();

  const table_schema = state;
  const table_name = "npmrds";

  const sql = dedent(
    pgFormat(
      `
        CREATE SCHEMA IF NOT EXISTS %I;

        CREATE TABLE IF NOT EXISTS %I.%I
          PARTITION OF %I.%I
          FOR VALUES IN (%L)
          PARTITION BY RANGE (date)
        ;
      `,
      table_schema,
      table_schema,
      table_name,
      parent_table_schema,
      parent_table_name,
      state
    )
  );

  await dama_db.query(sql);

  return { table_schema, table_name };
}
