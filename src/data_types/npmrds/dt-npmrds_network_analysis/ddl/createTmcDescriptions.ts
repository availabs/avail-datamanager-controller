import pgFormat from "pg-format";
import dedent from "dedent";

import dama_db from "data_manager/dama_db";

import {
  getNpmrdsNetworkEdgeMaxGeomIdxTableInfo,
  getNpmrdsNetworkEdgesMetadataViewInfo,
  getNpmrdsNetworkEdgesTableInfo,
  getNpmrdsNetworkNodeDescriptionsInfo,
  getNpmrdsNetworkPathsNodeIdxInfo,
  getTmcNetworkDescriptions,
  getTmcShapesTableInfo,
} from "./utils";

export default async function createView(year: number) {
  const edge_metadata_info = getNpmrdsNetworkEdgesMetadataViewInfo(year);
  const paths_node_idx_info = getNpmrdsNetworkPathsNodeIdxInfo(year);
  const max_geom_idx_table_info = getNpmrdsNetworkEdgeMaxGeomIdxTableInfo(year);
  const node_descriptions_info = getNpmrdsNetworkNodeDescriptionsInfo(year);
  const net_edge_info = getNpmrdsNetworkEdgesTableInfo(year);
  const tmc_shapes_info = getTmcShapesTableInfo(year);
  const tmc_network_descriptions_info = getTmcNetworkDescriptions(year);

  const sql = dedent(
    pgFormat(
      `
        DROP VIEW IF EXISTS %I.%I ;

        CREATE VIEW %I.%I       -- tmc_network_descriptions
          AS
            SELECT
                a.*,

                b.traversed_crossings_meta,

                COALESCE(c.other_tmcs, jsonb_build_array()) AS start_node_inbound_tmcs,
                COALESCE(d.other_tmcs, jsonb_build_array()) AS end_node_outbound_tmcs

              FROM %I.%I AS a   -- npmrds_network_edge_metadata
                INNER JOIN (
                  SELECT
                      x.tmc,

                      COALESCE(
                        jsonb_agg(
                          jsonb_build_object(
                            'node_id',                  node_id,
                            'linear_ids',               z.linear_ids,
                            'tmcs',                     z.tmcs,
                            'pt_geom_idx',              x.pt_geom_idx,
                            'firstnames',               z.firstnames,
                            'inbound_edges',            z.inbound_edges,
                            'outbound_edges',           z.outbound_edges,
                            'dist_along_path_meters',   x.dist_along_path_meters
                          ) 
                        ) FILTER ( WHERE ( array_length( z.linear_ids, 1 ) > 1 ) ),

                        jsonb_build_array()
                      ) AS traversed_crossings_meta

                    FROM %I.%I AS x                 -- npmrds_network_paths_node_idx
                      INNER JOIN %I.%I AS y         -- npmrds_network_edges_max_geom_idx
                        USING ( tmc )
                      LEFT OUTER JOIN %I.%I AS z    -- npmrds_network_node_descriptions
                        USING ( node_id )

                    GROUP BY tmc

                ) AS b USING ( tmc )

                LEFT OUTER JOIN LATERAL (
                  SELECT
                      x.tmc,
                      jsonb_agg(
                        jsonb_build_object(
                          'tmc',        z.tmc,
                          'linear_id',  z.linear_id,
                          'direction',  z.direction
                        ) ORDER BY z.tmc
                      )  AS other_tmcs

                    FROM %I.%I AS x                 -- npmrds_network_edges
                      INNER JOIN %I.%I AS y         -- npmrds_network_edges
                        USING (node_id)
                      INNER JOIN %I.%I AS z         -- tmc_shapes
                        ON ( y.tmc = z.tmc )
                    WHERE (
                      ( a.tmc = x.tmc )
                      AND
                      ( a.start_node_id = x.node_id )
                      AND
                      ( a.tmc != y.tmc )
                      AND
                      ( y.pt_geom_idx != 1 )
                      AND
                      -- Filter out TmcLinear u-turns
                      (
                        ( a.linear_id != z.linear_id )
                        OR
                        (
                          CASE ( a.direction )
                            WHEN 'NORTHBOUND' THEN ( z.direction != 'SOUTHBOUND' )
                            WHEN 'SOUTHBOUND' THEN ( z.direction != 'NORTHBOUND' )
                            WHEN 'EASTBOUND'  THEN ( z.direction != 'WESTBOUND'  )
                            WHEN 'WESTBOUND'  THEN ( z.direction != 'EASTBOUND'  )
                          END
                        )
                      )
                    )
                    GROUP BY x.tmc
                ) AS c USING ( tmc )
                LEFT OUTER JOIN LATERAL (
                  SELECT
                      w.tmc,
                      jsonb_agg(
                        jsonb_build_object(
                          'tmc',        z.tmc,
                          'linear_id',  z.linear_id,
                          'direction',  z.direction
                        ) ORDER BY z.tmc
                      )  AS other_tmcs

                    FROM %I.%I AS w               -- npmrds_network_edges
                      INNER JOIN %I.%I AS x       -- npmrds_network_edges
                        USING (node_id)
                      INNER JOIN %I.%I AS y       -- npmrds_network_edges_max_geom_idx
                        ON ( x.tmc = y.tmc )
                      INNER JOIN %I.%I AS z       -- tmc_shapes
                        ON ( x.tmc = z.tmc )
                    WHERE (
                      ( a.tmc = w.tmc )
                      AND
                      ( a.end_node_id = w.node_id )
                      AND
                      ( a.tmc != x.tmc )
                      AND
                      ( x.pt_geom_idx != y.max_pt_geom_idx )
                      AND
                      -- Filter out TmcLinear u-turns
                      (
                        ( a.linear_id != z.linear_id )
                        OR
                        (
                          CASE ( a.direction )
                            WHEN 'NORTHBOUND' THEN ( z.direction != 'SOUTHBOUND' )
                            WHEN 'SOUTHBOUND' THEN ( z.direction != 'NORTHBOUND' )
                            WHEN 'EASTBOUND'  THEN ( z.direction != 'WESTBOUND'  )
                            WHEN 'WESTBOUND'  THEN ( z.direction != 'EASTBOUND'  )
                          END
                        )
                      )
                    )
                    GROUP BY w.tmc
                ) AS d USING ( tmc )
        ;
        ;
      `,

      //  DROP VIEW
      tmc_network_descriptions_info.table_schema,
      tmc_network_descriptions_info.table_name,

      //  CREATE VIEW
      tmc_network_descriptions_info.table_schema,
      tmc_network_descriptions_info.table_name,

      //  FROM AS a
      edge_metadata_info.table_schema,
      edge_metadata_info.table_name,

      //  INNER JOIN AS b
      //    FROM AS x
      paths_node_idx_info.table_schema,
      paths_node_idx_info.table_name,
      //    FROM AS y
      max_geom_idx_table_info.table_schema,
      max_geom_idx_table_info.table_name,
      //    FROM AS z
      node_descriptions_info.table_schema,
      node_descriptions_info.table_name,

      //  INNER JOIN AS c
      //    FROM AS x
      net_edge_info.table_schema,
      net_edge_info.table_name,
      //    FROM AS y
      net_edge_info.table_schema,
      net_edge_info.table_name,
      //    FROM AS z
      tmc_shapes_info.table_schema,
      tmc_shapes_info.table_name,

      //  INNER JOIN AS d
      //    FROM AS w
      net_edge_info.table_schema,
      net_edge_info.table_name,
      //    FROM AS x
      net_edge_info.table_schema,
      net_edge_info.table_name,
      //    FROM AS y
      max_geom_idx_table_info.table_schema,
      max_geom_idx_table_info.table_name,
      //    FROM AS z
      tmc_shapes_info.table_schema,
      tmc_shapes_info.table_name
    )
  );

  await dama_db.query(sql);
}
