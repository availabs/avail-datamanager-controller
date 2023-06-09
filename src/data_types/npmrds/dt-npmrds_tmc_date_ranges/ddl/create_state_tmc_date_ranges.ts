import dedent from "dedent";
import pgFormat from "pg-format";

import dama_db from "data_manager/dama_db";

import { NpmrdsState } from "data_types/npmrds/domain";

import create_root_tmc_date_ranges_table from "./create_root_tmc_date_ranges_table";

export default async function main(state: NpmrdsState) {
  if (!NpmrdsState[state]) {
    throw new Error(`Invalid state: ${state}`);
  }

  const { table_schema: parent_table_schema, table_name: parent_table_name } =
    await create_root_tmc_date_ranges_table();

  const table_schema = state;

  const sql = dedent(
    pgFormat(
      `
        CREATE SCHEMA IF NOT EXISTS %I;

        CREATE TABLE IF NOT EXISTS %I.tmc_date_ranges
          PARTITION OF %I.%I (
            state DEFAULT %L,
            PRIMARY KEY (tmc)
          )
          FOR VALUES IN (%L)
        ;
      `,
      table_schema,
      table_schema,
      parent_table_schema,
      parent_table_name,
      state.toLowerCase(),
      state.toLowerCase()
    )
  );

  await dama_db.query(sql);

  const is_empty_sql = dedent(
    pgFormat(
      `
        SELECT NOT EXISTS (
          SELECT
              1
            FROM %I.tmc_date_ranges
        ) AS is_empty
      `,
      table_schema
    )
  );

  const {
    rows: [{ is_empty }],
  } = await dama_db.query(is_empty_sql);

  if (is_empty) {
    const cluster_sql = dedent(
      pgFormat(
        "CLUSTER %I.tmc_date_ranges USING tmc_date_ranges_pkey ;",
        table_schema
      )
    );

    await dama_db.query(cluster_sql);
  }

  return { table_schema, table_name: "tmc_date_ranges" };
}
