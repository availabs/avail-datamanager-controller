import dedent from "dedent";
import pgFormat from "pg-format";

import dama_db from "data_manager/dama_db";

import { NpmrdsState } from "data_types/npmrds/domain";

import create_root_tmc_identification_table_for_year from "./create_root_tmc_identification_table_for_year";

export default async function main(state: NpmrdsState, year: number) {
  if (!NpmrdsState[state]) {
    throw new Error(`Invalid state: ${state}`);
  }

  if (!/^\d{4}$/.test(`${year}`)) {
    throw new Error(`Invalid year: ${year}`);
  }

  const { table_schema: parent_table_schema, table_name: parent_table_name } =
    await create_root_tmc_identification_table_for_year(year);

  const table_schema = state;
  const table_name = `tmc_identification_${year}`;

  const sql = dedent(
    pgFormat(
      `
        CREATE SCHEMA IF NOT EXISTS %I;

        CREATE TABLE IF NOT EXISTS %I.%I
          PARTITION OF %I.%I
          FOR VALUES IN (%L)
          PARTITION BY LIST (state)
        ;
      `,
      table_schema,
      table_schema,
      table_name,
      parent_table_schema,
      parent_table_name,
      state.toUpperCase()
    )
  );

  await dama_db.query(sql);

  return { table_schema, table_name };
}
