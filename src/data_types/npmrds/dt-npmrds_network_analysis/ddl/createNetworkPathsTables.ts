import pgFormat from "pg-format";
import dedent from "dedent";

import dama_db from "data_manager/dama_db";
import logger from "data_manager/logger";

import {
  getNpmrdsNetworkEdgesTableInfo,
  getNpmrdsNetworkEdgesMetadataViewInfo,
  getNpmrdsNetworkPathsTableInfo,
} from "./utils";

async function createTable(year: number) {
  const paths_table_info = getNpmrdsNetworkPathsTableInfo(year);

  const sql = dedent(
    pgFormat(
      `
        DROP TABLE IF EXISTS %I.%I ;

        CREATE TABLE IF NOT EXISTS %I.%I (
          linear_id       INTEGER,
          direction       TEXT,

          path_idx        INTEGER,

          source_rank     INTEGER NOT NULL,
          depth           INTEGER NOT NULL,

          tmc             TEXT NOT NULL,

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
        WITH RECURSIVE cte_traverse(linear_id, direction, tmc, end_node_id, depth, path, source_rank) AS (
          SELECT
              a.linear_id,
              a.direction,
              a.tmc,
              a.end_node_id,
              0 AS depth,
              ARRAY[a.tmc] AS path,
              ROW_NUMBER()
                OVER (
                  PARTITION BY a.linear_id, a.direction --, a.county
                  ORDER BY
                    a.road_order,
                    CASE ( a.direction )
                      WHEN 'NORTHBOUND' THEN  a.start_latitude
                      WHEN 'SOUTHBOUND' THEN  -a.start_latitude
                      WHEN 'EASTBOUND'  THEN  -a.start_longitude
                      WHEN 'WESTBOUND'  THEN  a.start_longitude
                      ELSE LEAST(a.start_longitude, a.start_latitude) -- FIXME
                    END
              ) AS source_rank
            FROM %I.%I AS a
              LEFT OUTER JOIN %I.%I AS b
                ON (
                  ( a.linear_id = b.linear_id )
                  AND
                  ( a.direction = b.direction )
                  AND
                  ( a.start_node_id = b.end_node_id ) -- We are looking for network sources.
                )
            WHERE ( b.tmc IS NULL )
              
          UNION ALL

          SELECT
              a.linear_id,
              a.direction,
              a.tmc,
              a.end_node_id,
              ( b.depth + 1) AS depth,
              array_append(path, a.tmc) AS path,
              b.source_rank
            FROM %I.%I AS a
              INNER JOIN cte_traverse AS b
                ON (
                  ( a.linear_id = b.linear_id )
                  AND
                  ( a.direction = b.direction )
                  AND
                  ( a.start_node_id = b.end_node_id )
                )
            WHERE ( NOT (a.tmc = ANY(b.path)) )
        )
          INSERT INTO %I.%I (
            linear_id,
            direction,
            source_rank,
            depth,
            path_idx,
            tmc
          )
            SELECT
                linear_id,
                direction,
                source_rank,
                depth,
                ROW_NUMBER()
                  OVER (
                    PARTITION BY linear_id, direction
                    ORDER BY source_rank, depth
                  ) AS path_idx,
                tmc
              FROM cte_traverse
        ;


        CLUSTER %I.%I USING %I ;
      `,

      // cte_traverse 1st SELECT
      // FROM AS a
      edges_metadata_info.table_schema,
      edges_metadata_info.table_name,
      // FROM AS b
      edges_metadata_info.table_schema,
      edges_metadata_info.table_name,

      // cte_traverse 2nd SELECT
      // FROM AS a
      edges_metadata_info.table_schema,
      edges_metadata_info.table_name,

      // INSERT
      paths_table_info.table_schema,
      paths_table_info.table_name,

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

export default async function createNetworkPathsTable(year: number) {
  await createTable(year);
  await loadTable(year);
}
