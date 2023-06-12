import dedent from "dedent";
import pgFormat from "pg-format";

import dama_db from "data_manager/dama_db";

export default async function main(year: number) {
  if (!/^\d{4}$/.test(`${year}`)) {
    throw new Error(`Invalid year: ${year}`);
  }

  const table_schema = "public";
  const table_name = `tmc_identification_${year}`;

  const sql = dedent(
    pgFormat(
      `
        CREATE TABLE IF NOT EXISTS %I.%I (
          tmc                      CHARACTER VARYING,
          type                     CHARACTER VARYING,
          road                     CHARACTER VARYING,
          road_order               REAL,
          intersection             CHARACTER VARYING,
          tmclinear                INTEGER,
          country                  CHARACTER VARYING,
          state                    CHARACTER VARYING NOT NULL,
          county                   CHARACTER VARYING,
          zip                      CHARACTER VARYING,
          direction                CHARACTER VARYING,
          start_latitude           DOUBLE PRECISION,
          start_longitude          DOUBLE PRECISION,
          end_latitude             DOUBLE PRECISION,
          end_longitude            DOUBLE PRECISION,
          miles                    DOUBLE PRECISION,
          frc                      SMALLINT,
          border_set               CHARACTER VARYING,
          isprimary                SMALLINT,
          f_system                 SMALLINT,
          urban_code               INTEGER,
          faciltype                SMALLINT,
          structype                SMALLINT,
          thrulanes                SMALLINT,
          route_numb               INTEGER,
          route_sign               SMALLINT,
          route_qual               SMALLINT,
          altrtename               CHARACTER VARYING,
          aadt                     INTEGER,
          aadt_singl               INTEGER,
          aadt_combi               INTEGER,
          nhs                      SMALLINT,
          nhs_pct                  SMALLINT,
          strhnt_typ               SMALLINT,
          strhnt_pct               SMALLINT,
          truck                    SMALLINT,
          timezone_name            CHARACTER VARYING,
          active_start_date        DATE,
          active_end_date          DATE,
          download_timestamp       TIMESTAMP
        )
          PARTITION BY LIST (state)
        ;
      `,
      table_schema,
      table_name
    )
  );

  await dama_db.query(sql);

  return { table_schema, table_name };
}
