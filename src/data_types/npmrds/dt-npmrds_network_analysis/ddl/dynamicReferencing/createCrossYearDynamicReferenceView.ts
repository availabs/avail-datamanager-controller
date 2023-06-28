import pgFormat from "pg-format";
import dedent from "dedent";

import dama_db from "data_manager/dama_db";

import {
  getNpmrdsNetworkTmcDynamicReferenceInfo,
  getNpmrdsNetworkConformalMatchesTableInfo,
  getCrossYearDynamicReferenceInfo,
} from "../utils";

export default async function createView(year_a: number, year_b: number) {
  const tmc_dyn_ref_info_a = getNpmrdsNetworkTmcDynamicReferenceInfo(year_a);
  const tmc_dyn_ref_info_b = getNpmrdsNetworkTmcDynamicReferenceInfo(year_b);

  const matches_info = getNpmrdsNetworkConformalMatchesTableInfo(
    year_a,
    year_b
  );

  const cross_year_dyn_ref_info = getCrossYearDynamicReferenceInfo(
    year_a,
    year_b
  );

  const sql = dedent(
    pgFormat(
      `
        DROP VIEW IF EXISTS %I.%I ;

        CREATE VIEW %I.%I
          AS
            SELECT
                tmc,

                a.linear_id                           AS  linear_id_a,
                a.direction                           AS  direction_a,
                a.start_node_id                       AS  start_node_id_a,
                a.start_node_pt_geom_idx              AS  start_node_pt_geom_idx_a,
                a.start_node_idx_along_path           AS  start_node_idx_along_path_a,
                a.start_dist_along_path_meters        AS  start_dist_along_path_meters_a,
                a.end_node_id                         AS  end_node_id_a,
                a.end_node_idx                        AS  end_node_idx_a,
                a.end_node_idx_along_path             AS  end_node_idx_along_path_a,
                a.end_dist_along_path_meters          AS  end_dist_along_path_meters_a,
                a.reference_node_id                   AS  reference_node_id_a,
                a.reference_node_idx_along_path       AS  reference_node_idx_along_path_a,
                a.reference_node_dist_along_path      AS  reference_node_dist_along_path_a,
                a.reference_node_is_on_tmc            AS  reference_node_is_on_tmc_a,
                a.tmc_start_dist_from_reference_node  AS  tmc_start_dist_from_reference_node_a,
                a.tmc_end_dist_from_reference_node    AS  tmc_end_dist_from_reference_node_a,

                b.linear_id                           AS  linear_id_b,
                b.direction                           AS  direction_b,
                b.start_node_id                       AS  start_node_id_b,
                b.start_node_pt_geom_idx              AS  start_node_pt_geom_idx_b,
                b.start_node_idx_along_path           AS  start_node_idx_along_path_b,
                b.start_dist_along_path_meters        AS  start_dist_along_path_meters_b,
                b.end_node_id                         AS  end_node_id_b,
                b.end_node_idx                        AS  end_node_idx_b,
                b.end_node_idx_along_path             AS  end_node_idx_along_path_b,
                b.end_dist_along_path_meters          AS  end_dist_along_path_meters_b,
                b.reference_node_id                   AS  reference_node_id_b,
                b.reference_node_idx_along_path       AS  reference_node_idx_along_path_b,
                b.reference_node_dist_along_path      AS  reference_node_dist_along_path_b,
                b.reference_node_is_on_tmc            AS  reference_node_is_on_tmc_b,
                b.tmc_start_dist_from_reference_node  AS  tmc_start_dist_from_reference_node_b,
                b.tmc_end_dist_from_reference_node    AS  tmc_end_dist_from_reference_node_b,

                c.*,

                RANK() OVER (
                  PARTITION BY tmc
                  ORDER BY ABS(a.tmc_start_dist_from_reference_node)
                ) AS start_node_reference_rank_a,

                RANK() OVER (
                  PARTITION BY tmc
                  ORDER BY ABS(a.tmc_end_dist_from_reference_node)
                ) AS end_node_reference_rank_a,

                RANK() OVER (
                  PARTITION BY tmc
                  ORDER BY ABS(b.tmc_start_dist_from_reference_node)
                ) AS start_node_reference_rank_b,

                RANK() OVER (
                  PARTITION BY tmc
                  ORDER BY ABS(b.tmc_end_dist_from_reference_node)
                ) AS end_node_reference_rank_b

              FROM %I.%I AS a 
                INNER JOIN %I.%I AS b
                  USING ( tmc )
                INNER JOIN %I.%I AS c
                  ON (
                    ( a.reference_node_id = c.node_id_a )
                    AND
                    ( b.reference_node_id = c.node_id_b )
                    AND
                    (
                      NOT (
                        COALESCE(c.match_class, '')
                          IN (
                            'TMC_LINEAR_PATHS_OVERLAP_INTERNAL_NODES',
                            'MATCHED_TMC_NODE'                            -- TODO: ? Should we filter out ?
                          )
                      )
                    )
                  )
        ;
      `,

      //  DROP VIEW
      cross_year_dyn_ref_info.table_schema,
      cross_year_dyn_ref_info.table_name,

      //  CREATE VIEW
      cross_year_dyn_ref_info.table_schema,
      cross_year_dyn_ref_info.table_name,

      //  FROM AS a
      tmc_dyn_ref_info_a.table_schema,
      tmc_dyn_ref_info_a.table_name,

      //  INNER JOIN AS b
      tmc_dyn_ref_info_b.table_schema,
      tmc_dyn_ref_info_b.table_name,

      //  INNER JOIN AS c
      matches_info.table_schema,
      matches_info.table_name
    )
  );

  await dama_db.query(sql);
}
