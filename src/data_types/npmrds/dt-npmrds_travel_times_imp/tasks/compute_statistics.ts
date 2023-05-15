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
  const sql = dedent(
    pgFormat(
      `
        WITH cte_data_md5sum AS (

          SELECT
              MD5(
                string_agg(
                  MD5(
                    json_build_array(
                      tmc,
                      date,
                      epoch,
                      travel_time_all_vehicles,
                      travel_time_passenger_vehicles,
                      travel_time_freight_trucks,
                      data_density_all_vehicles,
                      data_density_passenger_vehicles,
                      data_density_freight_trucks
                    )::TEXT
                  ), '|' ORDER BY tmc)
              ) AS all_columns_md5sum,

              MD5(
                string_agg( DISTINCT tmc, '|' ORDER BY tmc)
              ) AS tmcs_md5sum

            FROM %I.%I
        ), cte_summary_by_frc AS (

          SELECT
              json_object_agg(
                COALESCE(frc::TEXT, 'null'),
                summary
              ) AS summary_by_frc
            FROM (
              SELECT
                  frc,
                  jsonb_build_object(
                    'total_tmcs',         COUNT( DISTINCT tmc ),
                    'total_records',      COUNT(1)
                  ) AS summary
                FROM %I.%I
                  INNER JOIN %I.%I
                    USING (tmc)
                GROUP BY frc
            ) AS t
        )
          SELECT
              *
            FROM cte_summary_by_frc AS a
              CROSS JOIN cte_data_md5sum AS b
      `,
      npmrds_travel_times_imp_table_schema,
      npmrds_travel_times_imp_table_name,
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
