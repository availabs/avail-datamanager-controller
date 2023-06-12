import dedent from "dedent";

import dama_db from "data_manager/dama_db";

export default async function main() {
  const sql = dedent(`
    CREATE TABLE IF NOT EXISTS public.tmc_date_ranges (
      tmc         VARCHAR(9),
      first_date  DATE,
      last_date   DATE,
      state       CHAR(2)
    ) PARTITION BY LIST (state) ;
  `);

  await dama_db.query(sql);

  return { table_schema: "public", table_name: "tmc_date_ranges" };
}
