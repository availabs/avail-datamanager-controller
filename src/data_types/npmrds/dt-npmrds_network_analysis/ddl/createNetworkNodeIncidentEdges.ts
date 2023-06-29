// TODO:  1. Identify Crossings, Merges, Splits
//        2. Create a table with Overlapping TmcLinear segments
//        3. DELETE "Intersections" between Merges and Splits
//        4. Get Bearings using https://postgis.net/docs/ST_PointN.html
//
// TODO: Document how because crossings may be TMC internal nodes, a TMC may be both INBOUND and OUTBOUND.

import pgFormat from "pg-format";
import dedent from "dedent";

import dama_db from "data_manager/dama_db";
import logger from "data_manager/logger";

import {
  getTmcShapesTableInfo,
  getNpmrdsNetworkNodesTableInfo,
  getNpmrdsNetworkEdgesTableInfo,
  getNpmrdsNetworkEdgeMaxGeomIdxTableInfo,
  getNpmrdsNetworkNodeIncidentEdgesInfo,
  getNpmrdsNetworkNodeIncidentEdgesMetadataInfo,
} from "./utils";

async function createTable(year: number) {
  const node_incident_edges_info = getNpmrdsNetworkNodeIncidentEdgesInfo(year);

  logger.debug(`Create ${node_incident_edges_info.table_name}: START`);

  const sql = dedent(
    pgFormat(
      `
        DROP TABLE IF EXISTS %I.%I CASCADE;

        CREATE TABLE IF NOT EXISTS %I.%I (
          node_id               INTEGER NOT NULL,

          tmc                   TEXT NOT NULL,
          pt_geom_idx           INTEGER NOT NULL,
          traversal_direction   TEXT NOT NULL CHECK( traversal_direction IN ('INBOUND', 'OUTBOUND') ),
          bearing               DOUBLE PRECISION,

          PRIMARY KEY ( node_id, tmc, pt_geom_idx, traversal_direction )

        ) WITH (fillfactor=100) ;

        CLUSTER %I.%I USING %I ;
        ;

      `,
      // DROP TABLE
      node_incident_edges_info.table_schema,
      node_incident_edges_info.table_name,

      // CREATE TABLE
      node_incident_edges_info.table_schema,
      node_incident_edges_info.table_name,

      // CLUSTER TABLE
      node_incident_edges_info.table_schema,
      node_incident_edges_info.table_name,
      node_incident_edges_info.pkey_idx_name
    )
  );

  await dama_db.query(sql);

  logger.debug(`Create ${node_incident_edges_info.table_name}: DONE`);
}

async function loadTablePass1(year: number) {
  const tmc_shapes_info = getTmcShapesTableInfo(year);
  const edges_table_info = getNpmrdsNetworkEdgesTableInfo(year);
  const edge_max_geom_idx_info = getNpmrdsNetworkEdgeMaxGeomIdxTableInfo(year);
  const node_incident_edges_info = getNpmrdsNetworkNodeIncidentEdgesInfo(year);

  logger.debug(
    `Load ${
      node_incident_edges_info.table_name
    }: START: ${new Date().toISOString()}`
  );

  // https://www.fusionbox.com/blog/detail/graph-algorithms-in-a-database-recursive-ctes-and-topological-sort-with-postgres/620/
  // https://www.postgresql.org/docs/11/queries-with.html
  const sql = dedent(
    pgFormat(
      `
        WITH cte_pseudo_intersections AS (
        -- FIXME: Filter out continuing along overlapping TMCs.
          -- TODO: Investigate why repeat (node_id, from_tmc, to_tmc) and the consequences.
          --       ? Self-intersecting TMCs?
          SELECT
              a.node_id AS node_id,

              a.tmc AS from_tmc,
              a.pt_geom_idx AS from_pt_geom_idx,

              b.tmc AS to_tmc,
              b.pt_geom_idx AS to_pt_geom_idx

            FROM %I.%I AS a                   -- npmrds_network_edges; The from/inbound TMCs
              INNER JOIN %I.%I AS x           -- tmc_shapes
                ON ( a.tmc = x.tmc )
              INNER JOIN %I.%I AS b           -- npmrds_network_edges; The to/outbound TMCs
                ON (
                  ( a.tmc != b.tmc )
                  AND
                  ( a.node_id = b.node_id )
                )
              INNER JOIN %I.%I AS y           -- tmc_shapes
                ON ( b.tmc = y.tmc )
              INNER JOIN %I.%I AS z           -- npmrds_network_edges_max_geom_idx
                ON ( ( b.tmc = z.tmc ) AND ( b.pt_geom_n = z.pt_geom_n ) )
            WHERE (
              (
                ( x.linear_id != y.linear_id )
                OR
                (
                  -- No continue straight along linear_id
                  ( x.direction != y.direction )
                  AND
                  -- No U-Turns if same linear_id
                  (
                    CASE ( x.direction )
                      WHEN 'NORTHBOUND' THEN ( y.direction != 'SOUTHBOUND' )
                      WHEN 'SOUTHBOUND' THEN ( y.direction != 'NORTHBOUND' )
                      WHEN 'EASTBOUND'  THEN ( y.direction != 'WESTBOUND'  )
                      WHEN 'WESTBOUND'  THEN ( y.direction != 'EASTBOUND'  )
                    END
                  )
                )
              )
              AND
              ( -- Node from from_tmc segment cannot be 1st node
                ( a.pt_geom_n = 1 ) -- filter out the MultiLineString higher N geometries for now.
                AND
                ( a.pt_geom_idx > 1 )
              )
              AND
              ( -- Node from to_tmc segment cannot be last node
                ( b.pt_geom_n = 1 ) -- filter out the MultiLineString higher N geometries for now.
                AND
                ( b.pt_geom_idx < z.max_pt_geom_idx )
              )
            )
        )
          INSERT INTO %I.%I (                             -- npmrds_network_node_incident_edges
            node_id,
            tmc,
            pt_geom_idx,
            traversal_direction
          )
            SELECT
                node_id,
                from_tmc AS tmc,
                from_pt_geom_idx AS pt_geom_idx,
                'INBOUND' AS traversal_direction
              FROM cte_pseudo_intersections
            UNION -- removes dupes
            SELECT
                node_id,
                to_tmc AS tmc,
                to_pt_geom_idx AS pt_geom_idx,
                'OUTBOUND' AS traversal_direction
              FROM cte_pseudo_intersections
        ;
      `,

      // cte_pseudo_intersections FROM a
      edges_table_info.table_schema,
      edges_table_info.table_name,
      // cte_pseudo_intersections INNER JOIN x
      tmc_shapes_info.table_schema,
      tmc_shapes_info.table_name,
      // cte_pseudo_intersections INNER JOIN b
      edges_table_info.table_schema,
      edges_table_info.table_name,
      // cte_pseudo_intersections INNER JOIN y
      tmc_shapes_info.table_schema,
      tmc_shapes_info.table_name,
      // cte_pseudo_intersections INNER JOIN z
      edge_max_geom_idx_info.table_schema,
      edge_max_geom_idx_info.table_name,

      // INSERT INTO
      node_incident_edges_info.table_schema,
      node_incident_edges_info.table_name
    )
  );

  await dama_db.query(sql);

  logger.debug(
    `Load ${
      node_incident_edges_info.table_name
    }: DONE:  ${new Date().toISOString()}`
  );
}

async function loadTablePass2(year: number) {
  const tmc_shapes_info = getTmcShapesTableInfo(year);
  const edges_table_info = getNpmrdsNetworkEdgesTableInfo(year);
  const edge_max_geom_idx_info = getNpmrdsNetworkEdgeMaxGeomIdxTableInfo(year);
  const node_incident_edges_info = getNpmrdsNetworkNodeIncidentEdgesInfo(year);

  logger.debug(
    `Load ${
      node_incident_edges_info.table_name
    }: START: ${new Date().toISOString()}`
  );

  // https://www.fusionbox.com/blog/detail/graph-algorithms-in-a-database-recursive-ctes-and-topological-sort-with-postgres/620/
  // https://www.postgresql.org/docs/11/queries-with.html
  const sql = dedent(
    pgFormat(
      `
        WITH cte_from_segments AS (
          SELECT
              node_id,
              tmc,
              pt_geom_idx,
              traversal_direction,
              -- https://postgis.net/docs/ST_MakeLine.html
              ST_MakeLine(
                (pt_dump).geom
                ORDER BY (pt_dump).path[2]
              ) AS line_segment
            FROM (
              SELECT
                  a.node_id,
                  tmc,
                  a.pt_geom_idx,
                  a.traversal_direction,
                  ST_DumpPoints(b.wkb_geometry) AS pt_dump
                FROM %I.%I AS a                               -- npmrds_network_node_incident_edges
                  INNER JOIN %I.%I AS b                       -- tmc_shapes
                    USING (tmc)
                WHERE ( a.traversal_direction = 'INBOUND' )
            ) AS t

            WHERE (
              ( (pt_dump).path[1] = 1 )
              AND
              ( (pt_dump).path[2] <= pt_geom_idx )
            )

            GROUP BY node_id, tmc, pt_geom_idx, traversal_direction
        ), cte_from_bearings AS (
          SELECT
              node_id,
              tmc,
              pt_geom_idx,
              traversal_direction,

              Round(
                DEGREES(
                  ST_Azimuth(
                    ST_StartPoint(
                      ST_LineSubstring(
                        line_segment,

                        -- startfraction
                        (
                          GREATEST(
                            0,
                            ST_Length(
                              GEOGRAPHY( line_segment )
                            ) - 20
                          )
                          /
                          ST_Length(
                            GEOGRAPHY( line_segment )
                          )
                        ),

                        -- endfraction
                        1
                      )
                    ),
                    ST_EndPoint( line_segment )
                  )
                )::NUMERIC,
                2
              ) AS bearing

            FROM cte_from_segments

        ), cte_to_segments AS (
          SELECT
              node_id,
              tmc,
              pt_geom_idx,
              traversal_direction,

              -- https://postgis.net/docs/ST_MakeLine.html
              ST_MakeLine(
                (pt_dump).geom
                ORDER BY (pt_dump).path[2]
              ) AS line_segment

            FROM (
              SELECT
                  a.node_id,
                  tmc,
                  a.pt_geom_idx,
                  a.traversal_direction,
                  ST_DumpPoints(b.wkb_geometry) AS pt_dump
                FROM %I.%I AS a                               -- npmrds_network_node_incident_edges
                  INNER JOIN %I.%I AS b                       -- tmc_shapes
                    USING (tmc)
                WHERE ( a.traversal_direction = 'OUTBOUND' )
            ) AS t

            WHERE (
              ( (pt_dump).path[1] = 1 )
              AND
              ( (pt_dump).path[2] >= pt_geom_idx )
            )

            GROUP BY node_id, tmc, pt_geom_idx, traversal_direction
        ), cte_to_bearings AS (
          SELECT
              node_id,
              tmc,
              pt_geom_idx,
              traversal_direction,

              ROUND(
                DEGREES(
                  ST_Azimuth(
                    ST_StartPoint( line_segment ),

                    ST_EndPoint(
                      ST_LineSubstring(
                        line_segment,

                        -- startfraction
                        0,

                        --endfraction
                        (
                          LEAST(
                            20,
                            ST_Length(
                              GEOGRAPHY( line_segment )
                            )
                          )
                          /
                          ST_Length(
                            GEOGRAPHY( line_segment )
                          )
                        )
                      )
                    )
                  )
                )::NUMERIC,
                2
              ) AS bearing

            FROM cte_to_segments
        )
          UPDATE %I.%I AS a
            SET
                bearing = b.bearing
            FROM (
              SELECT
                  node_id,
                  tmc,
                  pt_geom_idx,
                  traversal_direction,
                  bearing
                FROM cte_from_bearings

              UNION ALL

              SELECT
                  node_id,
                  tmc,
                  pt_geom_idx,
                  traversal_direction,
                  bearing
                FROM cte_to_bearings
            ) AS b
            WHERE (
              ( a.node_id = b.node_id )
              AND
              ( a.tmc = b.tmc )
              AND
              ( a.pt_geom_idx = b.pt_geom_idx )
              AND
              ( a.traversal_direction = b.traversal_direction )
            )
        ;
      `,

      //  WITH cte_from_segments
      //    FROM a
      node_incident_edges_info.table_schema,
      node_incident_edges_info.table_name,
      //    FROM b
      tmc_shapes_info.table_schema,
      tmc_shapes_info.table_name,

      //  WITH cte_to_segments
      //    FROM a
      node_incident_edges_info.table_schema,
      node_incident_edges_info.table_name,
      //    FROM b
      tmc_shapes_info.table_schema,
      tmc_shapes_info.table_name,

      // INSERT INTO
      node_incident_edges_info.table_schema,
      node_incident_edges_info.table_name
    )
  );

  await dama_db.query(sql);

  logger.debug(
    `Load ${
      node_incident_edges_info.table_name
    }: DONE:  ${new Date().toISOString()}`
  );
}

async function loadTable(year: number) {
  await loadTablePass1(year);
  await loadTablePass2(year);
}

async function createMetadataView(year: number) {
  const tmc_shapes_info = getTmcShapesTableInfo(year);
  const nodes_table_info = getNpmrdsNetworkNodesTableInfo(year);
  const node_incident_edges_info = getNpmrdsNetworkNodeIncidentEdgesInfo(year);
  const edge_max_geom_idx_info = getNpmrdsNetworkEdgeMaxGeomIdxTableInfo(year);

  const node_incident_edges_metadata_info =
    getNpmrdsNetworkNodeIncidentEdgesMetadataInfo(year);

  logger.debug(`Create ${node_incident_edges_metadata_info.table_name}: START`);

  const sql = dedent(
    pgFormat(
      `
        DROP VIEW IF EXISTS %I.%I CASCADE ;

        CREATE VIEW %I.%I                       -- npmrds_network_node_incident_edges_metadata
          AS
            SELECT
                a.node_id,
                b.longitude,
                b.latitude,

                a.tmc,
                a.pt_geom_idx,
                a.traversal_direction,
                a.bearing,

                c.linear_id,
                c.roadnumber,
                c.roadname,
                c.direction,
                c.county,

                CASE
                  WHEN ( a.pt_geom_idx = d.max_pt_geom_idx )
                    THEN c.firstname
                  ELSE NULL
                END AS firstname,

                b.wkb_geometry

              FROM %I.%I AS a                   -- npmrds_network_node_incident_edges
                INNER JOIN %I.%I AS b           -- npmrds_network_nodes
                  USING ( node_id )
                INNER JOIN %I.%I AS c           -- tmc_shapes
                  USING ( tmc )
                INNER JOIN %I.%I AS d           -- npmrds_network_edges_max_geom_idx
                  USING ( tmc )
        ;
      `,

      // DROP VIEW
      node_incident_edges_metadata_info.table_schema,
      node_incident_edges_metadata_info.table_name,

      //  CREATE VIEW
      node_incident_edges_metadata_info.table_schema,
      node_incident_edges_metadata_info.table_name,
      //    FROM a
      node_incident_edges_info.table_schema,
      node_incident_edges_info.table_name,
      //    INNER JOIN b
      nodes_table_info.table_schema,
      nodes_table_info.table_name,
      //    INNER JOIN c
      tmc_shapes_info.table_schema,
      tmc_shapes_info.table_name,
      //    INNER JOIN d
      edge_max_geom_idx_info.table_schema,
      edge_max_geom_idx_info.table_name
    )
  );

  await dama_db.query(sql);

  logger.debug(`Create ${node_incident_edges_metadata_info.table_name}: DONE`);
}

export default async function createNetworkNodeIncidentEdges(year: number) {
  await createTable(year);
  await loadTable(year);
  await createMetadataView(year);
}
