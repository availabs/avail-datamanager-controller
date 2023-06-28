import dedent from "dedent";
import pgFormat from "pg-format";

import dama_db from "data_manager/dama_db";

import {
  getNpmrdsNetworkEdgesMetadataViewInfo,
  getNpmrdsNetworkPathsNodeIdxInfo,
  getNpmrdsNetworkTmcDynamicReferenceInfo,
} from "./utils";

export default async function createView(year: number) {
  const edges_meta_info = getNpmrdsNetworkEdgesMetadataViewInfo(year);
  const node_idx_info = getNpmrdsNetworkPathsNodeIdxInfo(year);
  const tmc_dyn_info = getNpmrdsNetworkTmcDynamicReferenceInfo(year);

  const sql = dedent(
    pgFormat(
      `
        DROP VIEW IF EXISTS %I.%I CASCADE ;

        CREATE VIEW %I.%I
          AS
            SELECT DISTINCT ON (tmc, reference_node_idx_along_path)
                a.tmc,

                linear_id,
                direction,

                a.start_node_id,
                1 AS start_node_pt_geom_idx,

                b.node_idx_along_path AS start_node_idx_along_path,
                b.dist_along_path_meters AS start_dist_along_path_meters,

                a.end_node_id,
                a.end_node_idx,

                c.node_idx_along_path AS end_node_idx_along_path,
                c.dist_along_path_meters AS end_dist_along_path_meters,

                d.node_id AS reference_node_id,
                d.node_idx_along_path AS reference_node_idx_along_path,
                d.dist_along_path_meters AS reference_node_dist_along_path,
                
                ( a.tmc = d.tmc ) AS reference_node_is_on_tmc,

                ( b.dist_along_path_meters - d.dist_along_path_meters ) AS tmc_start_dist_from_reference_node,
                ( c.dist_along_path_meters - d.dist_along_path_meters ) AS tmc_end_dist_from_reference_node

              FROM %I.%I AS a
                INNER JOIN %I.%I AS b
                  USING (linear_id, direction, tmc)
                INNER JOIN %I.%I AS c
                  USING (linear_id, direction, tmc)
                INNER JOIN %I.%I AS d
                  USING ( linear_id, direction )

              WHERE (
                (
                  ( a.start_node_id = b.node_id )
                  AND
                  ( 1 = b.pt_geom_idx )
                )
                AND
                (
                  ( a.end_node_id = c.node_id )
                  AND
                  ( a.end_node_idx = c.pt_geom_idx )
                )
              )
              ORDER BY
                  tmc,
                  reference_node_idx_along_path,
                  ( a.tmc = d.tmc )::INTEGER DESC
        ;
      `,
      //  DROP VIEW
      tmc_dyn_info.table_schema,
      tmc_dyn_info.table_name,

      //  CREATE VIEW
      tmc_dyn_info.table_schema,
      tmc_dyn_info.table_name,

      //    FROM AS a
      edges_meta_info.table_schema,
      edges_meta_info.table_name,

      //    INNER JOIN AS b
      node_idx_info.table_schema,
      node_idx_info.table_name,

      //    INNER JOIN AS c
      node_idx_info.table_schema,
      node_idx_info.table_name,

      //    INNER JOIN AS d
      node_idx_info.table_schema,
      node_idx_info.table_name
    )
  );

  await dama_db.query(sql);
}
