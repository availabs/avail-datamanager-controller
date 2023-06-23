import pgFormat from "pg-format";
import dedent from "dedent";

import dama_db from "data_manager/dama_db";
import logger from "data_manager/logger";

import {
  getNpmrdsNetworkEdgesTableInfo,
  getNpmrdsNetworkEdgesMetadataViewInfo,
  getNpmrdsNetworkPathsTableInfo,
  getNpmrdsNetworkPathsNodeIdxViewInfo,
} from "./utils";

async function createTable(year: number) {
  const paths_table_info = getNpmrdsNetworkPathsTableInfo(year);

  const sql = dedent(
    pgFormat(
      `
        DROP TABLE IF EXISTS %I.%I CASCADE ;

        CREATE TABLE IF NOT EXISTS %I.%I (
          linear_id       INTEGER,
          direction       TEXT,

          path_idx        INTEGER,

          tmc             TEXT NOT NULL,

          -- NOTE: FOR 2019, linear_ids 12031623 & 12031626 have NULL road_order for ALL TMCs.
          road_order      INTEGER,

          PRIMARY KEY (linear_id, direction, path_idx)
        ) ;
      `,

      // DROP TABLE
      paths_table_info.table_schema,
      paths_table_info.table_name,

      // CREATE TABLE
      paths_table_info.table_schema,
      paths_table_info.table_name
    )
  );

  await dama_db.query(sql);
}

async function loadTable(year: number) {
  const edges_metadata_info = getNpmrdsNetworkEdgesMetadataViewInfo(year);
  const paths_table_info = getNpmrdsNetworkPathsTableInfo(year);

  logger.debug(
    `Load ${paths_table_info.table_name} START: ${new Date().toISOString()}`
  );

  // https://www.fusionbox.com/blog/detail/graph-algorithms-in-a-database-recursive-ctes-and-topological-sort-with-postgres/620/
  // https://www.postgresql.org/docs/11/queries-with.html
  const sql = dedent(
    pgFormat(
      `
        INSERT INTO %I.%I (
          linear_id,
          direction,
          path_idx,
          tmc,
          road_order
        )
          SELECT
              a.linear_id,
              a.direction,

              ROW_NUMBER()
                OVER (
                  PARTITION BY a.linear_id, a.direction --, a.county
                  ORDER BY
                    a.road_order,
                    -- TODO: How reliable are the start/end lat/lon from the tmc_shapes ?
                    --       How well do they handle MultiLineStrings where geometries > 1 ?
                    CASE ( a.direction )
                      WHEN 'NORTHBOUND' THEN  a.start_latitude
                      WHEN 'SOUTHBOUND' THEN  -a.start_latitude
                      WHEN 'EASTBOUND'  THEN  -a.start_longitude
                      WHEN 'WESTBOUND'  THEN  a.start_longitude
                      ELSE LEAST(a.start_longitude, a.start_latitude) -- FIXME
                    END
              ) AS path_idx,

              a.tmc,
              a.road_order

            FROM %I.%I AS a
        ;

        CLUSTER %I.%I USING %I ;
      `,

      // INSERT
      paths_table_info.table_schema,
      paths_table_info.table_name,

      // cte_traverse 1st SELECT
      // FROM AS a
      edges_metadata_info.table_schema,
      edges_metadata_info.table_name,

      // CLUSTER TABLE
      paths_table_info.table_schema,
      paths_table_info.table_name,
      paths_table_info.pkey_idx_name
    )
  );

  await dama_db.query(sql);

  logger.debug(
    `Load ${paths_table_info.table_name} START: ${new Date().toISOString()}`
  );
}

async function createView(year: number) {
  const edges_info = getNpmrdsNetworkEdgesTableInfo(year);
  const paths_info = getNpmrdsNetworkPathsTableInfo(year);
  const view_info = getNpmrdsNetworkPathsNodeIdxViewInfo(year);

  const sql = dedent(
    pgFormat(
      `
        DROP VIEW IF EXISTS %I.%I CASCADE ;

        CREATE VIEW %I.%I
          AS
            WITH cte_bar AS (
              SELECT
                  a.linear_id,
                  a.direction,

                  a.tmc AS tmc_a,
                  a.road_order AS road_order_a,
                  c.pt_geom_idx AS v_geom_idx_a,
                  d.pt_geom_idx AS w_geom_idx_a,

                  b.tmc AS tmc_b,
                  b.road_order AS road_order_b,
                  e.pt_geom_idx AS v_geom_idx_b,
                  f.pt_geom_idx AS w_geom_idx_b,

                  ARRAY[c.node_id, d.node_id] AS node_pair

                FROM %I.%I AS a
                  INNER JOIN %I.%I AS b
                    ON (
                      ( a.linear_id = b.linear_id )
                      AND
                      ( a.direction = b.direction )
                      AND
                      ( a.path_idx < b.path_idx )
                    )
                  INNER JOIN %I.%I AS c
                    ON (
                      ( a.tmc = c.tmc )
                      AND
                      ( c.pt_geom_n = 1 )
                    )
                  INNER JOIN %I.%I AS d
                    ON (
                      ( a.tmc = d.tmc )
                      AND
                      ( d.pt_geom_n = 1 )
                      AND
                      ( ( c.pt_geom_idx + 1 ) = d.pt_geom_idx )
                    )
                  INNER JOIN %I.%I AS e
                    ON (
                      ( b.tmc = e.tmc )
                      AND
                      ( e.pt_geom_n = 1 )
                    )
                  INNER JOIN %I.%I AS f
                    ON (
                      ( b.tmc = f.tmc )
                      AND
                      ( f.pt_geom_n = 1 )
                      AND
                      ( ( e.pt_geom_idx + 1 ) = f.pt_geom_idx )
                    )
                WHERE ( (c.node_id, d.node_id) = (e.node_id, f.node_id) )
            ), cte_1 AS (
              SELECT
                  a.linear_id,
                  a.direction,

                  a.tmc,
                  a.road_order,

                  a.path_idx,
                  b.pt_geom_idx,

                  b.node_id,

                  LAG(b.node_id, 1) OVER (
                    PARTITION BY a.linear_id, a.direction
                    ORDER BY a.path_idx, b.pt_geom_idx
                  ) AS prev_node_id

                FROM %I.%I AS a
                  INNER JOIN %I.%I AS b
                    USING (tmc)

                WHERE ( b.pt_geom_n = 1 )
            ), cte_2 AS (
              SELECT
                  a.*
                FROM cte_1 AS a
                  LEFT OUTER JOIN cte_bar AS b
                    ON (
                      ( a.tmc = b.tmc_b )
                      AND
                      (
                        ( a.pt_geom_idx = b.v_geom_idx_b )
                        OR
                        ( a.pt_geom_idx = b.w_geom_idx_b )
                      )
                    )
                WHERE ( b.tmc_b IS NULL )
            ), cte_3 AS (
              SELECT
                  a.*,
                  b.tmc_a,
                  b.v_geom_idx_a,
                  b.w_geom_idx_a
                FROM cte_1 AS a
                  INNER JOIN cte_bar AS b
                    ON (
                      ( a.tmc = b.tmc_b )
                      AND
                      (
                        ( a.pt_geom_idx = b.v_geom_idx_b )
                        OR
                        ( a.pt_geom_idx = b.w_geom_idx_b )
                      )
                    )
            ), cte_4 AS (
              SELECT
                  linear_id,
                  direction,

                  tmc,
                  road_order,

                  path_idx,
                  pt_geom_idx,

                  node_id,

                  -- https://stackoverflow.com/a/62019700
                  SUM(
                    CASE
                      WHEN ( node_id = prev_node_id ) THEN 0
                      ELSE 1
                    END
                  ) OVER (
                    PARTITION BY linear_id, direction
                    ORDER BY path_idx, pt_geom_idx
                  ) AS node_idx_along_path

                FROM cte_2
            ), cte_5 AS (
              SELECT DISTINCT ON ( tmc, pt_geom_idx )
                  a.linear_id,
                  a.direction,

                  a.tmc,
                  a.road_order,

                  a.path_idx,
                  a.pt_geom_idx,

                  node_id,

                  b.node_idx_along_path
                FROM cte_3 AS a
                  INNER JOIN cte_4 AS b
                    USING ( linear_id, direction, node_id )
                WHERE (
                  ( a.tmc_a = b.tmc )
                  AND
                  (
                    ( a.v_geom_idx_a = b.pt_geom_idx )
                    OR
                    ( a.w_geom_idx_a = b.pt_geom_idx )
                  )
                )
                ORDER BY tmc, pt_geom_idx
            )
              SELECT * FROM cte_4
              UNION ALL
              SELECT * FROM cte_5
      `,

      //  DROP VIEW
      view_info.table_schema,
      view_info.table_name,

      //  CREATE VIEW
      view_info.table_schema,
      view_info.table_name,

      //  cte_bar
      //    FROM AS a
      paths_info.table_schema,
      paths_info.table_name,
      //    INNER JOIN AS b
      paths_info.table_schema,
      paths_info.table_name,
      //    INNER JOIN AS c
      edges_info.table_schema,
      edges_info.table_name,
      //    INNER JOIN AS d
      edges_info.table_schema,
      edges_info.table_name,
      //    INNER JOIN AS e
      edges_info.table_schema,
      edges_info.table_name,
      //    INNER JOIN AS f
      edges_info.table_schema,
      edges_info.table_name,

      //  cte_1
      //    FROM AS a
      paths_info.table_schema,
      paths_info.table_name,
      //    INNER JOIN AS b
      edges_info.table_schema,
      edges_info.table_name
    )
  );

  await dama_db.query(sql);
}

export default async function createNetworkPathsTable(year: number) {
  await createTable(year);
  await loadTable(year);
  await createView(year);
}
