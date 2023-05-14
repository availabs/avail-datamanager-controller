import dedent from "dedent";
import pgFormat from "pg-format";

import dama_db from "data_manager/dama_db";

export default async function main() {
  const table_schema = "public";
  const table_name = "npmrds";

  const sql = dedent(
    pgFormat(
      `
        CREATE TABLE IF NOT EXISTS %I.%I (
          tmc                               VARCHAR(9),
          date                              DATE,
          epoch                             SMALLINT,
          travel_time_all_vehicles          REAL,
          travel_time_passenger_vehicles    REAL,
          travel_time_freight_trucks        REAL,
          data_density_all_vehicles         CHAR,
          data_density_passenger_vehicles   CHAR,
          data_density_freight_trucks       CHAR,
          state                             CHAR(2) NOT NULL
        )
          PARTITION BY LIST (state)
        ;
      `,
      table_schema,
      table_name
    )
  );

  await dama_db.query(sql);

  return {
    table_schema: "public",
    table_name: "npmrds",
  };
}
