import dedent from "dedent";
import pgFormat from "pg-format";

import dama_db from "data_manager/dama_db";

export type DoneData = {
  all_columns_md5sum: string;
  tmcs_md5sum: string;
  summary_by_frc: Record<
    number,
    {
      total_tmcs: number;
      total_records: number;
    }
  >;
};

export default async function main(
  npmrds_travel_times_imp_table_schema: string,
  npmrds_travel_times_imp_table_name: string,
  tmc_identification_imp_table_schema: string,
  tmc_identification_imp_table_name: string
): Promise<DoneData> {
  // NOTE: Encountered the following error when getting to aggressive with the string_agg
  //
  //       ERROR:  out of memory
  //       DETAIL: Cannot enlarge string buffer containing 1073741822 bytes by 1 more bytes.
  //
  const sql = dedent(
    pgFormat(
      `
        SELECT
            json_object_agg(
              COALESCE(frc::TEXT, 'null'),
              summary
            ) AS summary_by_frc
          FROM (
            SELECT
                frc,
                jsonb_build_object(
                  'total_tmcs',               COUNT( DISTINCT tmc ),
                  'total_records',            COUNT(1),
                  'tmcs_md5sum',              MD5(string_agg( DISTINCT tmc, '|' ORDER BY TMC)),
                  'travel_time_all_sum',      SUM(travel_time_all_vehicles::NUMERIC)::TEXT
                ) AS summary
              FROM %I.%I
                INNER JOIN %I.%I
                  USING (tmc)
              GROUP BY frc
          ) AS t
      `,
      npmrds_travel_times_imp_table_schema,
      npmrds_travel_times_imp_table_name,
      tmc_identification_imp_table_schema,
      tmc_identification_imp_table_name
    )
  );

  const {
    rows: [statistics],
  } = await dama_db.query(sql);

  return statistics;
}
