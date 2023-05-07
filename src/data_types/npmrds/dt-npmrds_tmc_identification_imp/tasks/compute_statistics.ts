import dedent from "dedent";
import pgFormat from "pg-format";

import dama_db from "data_manager/dama_db";

import { DoneData as LoadDoneData } from "./load";

export type DoneData = {
  all_columns_md5sum: string;
  tmcs_md5sum: string;
  miles_md5sum: string;
  aadt_md5sum: string;
  coords_md5sum: string;
  summary_by_frc: Record<
    number,
    {
      total_tmcs: number;
      total_miles: number;
    }
  >;
};

export default async function main(
  load_done_data: LoadDoneData
): Promise<DoneData> {
  const { table_schema, table_name } = load_done_data;

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
                      type,
                      road,
                      road_order,
                      intersection,
                      tmclinear,
                      country,
                      state,
                      county,
                      zip,
                      direction,
                      start_latitude,
                      start_longitude,
                      end_latitude,
                      end_longitude,
                      miles,
                      frc,
                      border_set,
                      isprimary,
                      f_system,
                      urban_code,
                      faciltype,
                      structype,
                      thrulanes,
                      route_numb,
                      route_sign,
                      route_qual,
                      altrtename,
                      aadt,
                      aadt_singl,
                      aadt_combi,
                      nhs,
                      nhs_pct,
                      strhnt_typ,
                      strhnt_pct,
                      truck,
                      timezone_name
                    )::TEXT
                  ), '|' ORDER BY  tmc)
              ) AS all_columns_md5sum,

              MD5(
                string_agg( tmc, '|' ORDER BY tmc)
              ) AS tmcs_md5sum,

              MD5(
                string_agg(miles::TEXT, '|' ORDER BY tmc)
              ) AS miles_md5sum,

              MD5(
                string_agg(aadt::TEXT,  '|'  ORDER BY tmc)
              ) AS aadt_md5sum,

              MD5(
                string_agg(
                  start_longitude::TEXT
                    ||  ','
                    ||  start_latitude::TEXT
                    ||  ' '
                    ||  end_longitude::TEXT
                    ||  ','
                    ||  end_latitude::TEXT,
                  '|'
                  ORDER BY tmc
                )
              ) AS coords_md5sum
            FROM %I.%I
        ), cte_summary_by_frc AS (

          SELECT
              json_object_agg(
                frc,
                summary
              ) AS summary_by_frc
            FROM (
              SELECT
                  frc,
                  jsonb_build_object(
                    'total_tmcs',     COUNT(1),
                    'total_miles',    SUM(miles)
                  ) AS summary
                FROM %I.%I
                GROUP BY frc
            ) AS t
        )
          SELECT
              *
            FROM cte_summary_by_frc AS a
              CROSS JOIN cte_data_md5sum AS b
      `,
      table_schema,
      table_name,
      table_schema,
      table_name
    )
  );

  const {
    rows: [statistics],
  } = await dama_db.query(sql);

  return statistics;
}
