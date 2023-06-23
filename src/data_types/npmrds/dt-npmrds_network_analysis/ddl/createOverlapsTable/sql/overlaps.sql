DROP TABLE IF EXISTS npmrds_network_spatial_analysis.npmrds_network_path_overlaps_:YEAR CASCADE ;

CREATE TABLE npmrds_network_spatial_analysis.npmrds_network_path_overlaps_:YEAR (
    overlap_id                      SERIAL PRIMARY KEY,

    linear_id_a                     INTEGER NOT NULL,
    direction_a                     TEXT NOT NULL,

    linear_id_b                     INTEGER NOT NULL,
    direction_b                     TEXT NOT NULL,

    start_node_id                   INTEGER NOT NULL,
    end_node_id                     INTEGER NOT NULL,

    start_node_idx_along_path_a     INTEGER NOT NULL,
    end_node_idx_along_path_a       INTEGER NOT NULL,

    start_node_idx_along_path_b     INTEGER NOT NULL,
    end_node_idx_along_path_b       INTEGER NOT NULL,

    tmcs_a        TEXT[],
    tmcs_b        TEXT[],

    node_ids      INTEGER[],

    CONSTRAINT npmrds_network_path_overlaps_:YEAR_uniq
      UNIQUE (linear_id_a, direction_a, linear_id_b, direction_b, start_node_id, end_node_id)

) WITH (fillfactor=100) ;

WITH RECURSIVE cte_shared_nodes AS (
  -- Collect all nodes shared between two TmcLinears
  SELECT DISTINCT ON (  node_id,
                        linear_id_a,
                        direction_a,
                        linear_id_b,
                        direction_b,
                        node_idx_along_path_a,
                        node_idx_along_path_b
                     )
        node_id,

        a.linear_id AS linear_id_a,
        a.direction AS direction_a,

        b.linear_id AS linear_id_b,
        b.direction AS direction_b,

        a.node_idx_along_path AS node_idx_along_path_a,
        b.node_idx_along_path AS node_idx_along_path_b

      FROM npmrds_network_spatial_analysis.npmrds_network_paths_node_idx_:YEAR AS a
        INNER JOIN npmrds_network_spatial_analysis.npmrds_network_paths_node_idx_:YEAR AS b
          USING ( node_id )
      WHERE ( a.linear_id < b.linear_id )
      ORDER BY
          node_id,
          linear_id_a,
          direction_a,
          linear_id_b,
          direction_b,
          node_idx_along_path_a,
          node_idx_along_path_b

), cte_overlapping_node_sequences AS (
  -- Recursively expand the overlapping nodes ranges for each TmcLinear.
  SELECT
      linear_id_a,
      direction_a,

      linear_id_b,
      direction_b,

      node_id AS start_node_id,
      node_id AS end_node_id,

      0 AS overlap_n,

      node_idx_along_path_a AS start_node_idx_along_path_a,
      node_idx_along_path_a AS end_node_idx_along_path_a,

      node_idx_along_path_b AS start_node_idx_along_path_b,
      node_idx_along_path_b AS end_node_idx_along_path_b

    FROM cte_shared_nodes
  
  UNION

  SELECT
      linear_id_a,
      direction_a,

      linear_id_b,
      direction_b,

      a.start_node_id,
      b.node_id AS end_node_id,

      ( a.overlap_n + 1 ) AS overlap_n,

      a.start_node_idx_along_path_a,
      b.node_idx_along_path_a AS end_node_idx_along_path_a,

      a.start_node_idx_along_path_b,
      b.node_idx_along_path_b AS end_node_idx_along_path_b

    FROM cte_overlapping_node_sequences AS a
      INNER JOIN cte_shared_nodes AS b
        USING (linear_id_a, direction_a, linear_id_b, direction_b)

    -- Here we step one node at a time along the TmcLinear Paths.
    WHERE (
      ( ( a.end_node_idx_along_path_a + 1 ) = b.node_idx_along_path_a )
      AND
      ( ( a.end_node_idx_along_path_b + 1 ) = b.node_idx_along_path_b )
    )
), cte_max_overlapping_node_sequences AS (
    SELECT DISTINCT
        a.linear_id_a,
        a.direction_a,

        a.linear_id_b,
        a.direction_b,

        a.start_node_id,
        a.end_node_id,

        a.start_node_idx_along_path_a,
        a.end_node_idx_along_path_a,

        a.start_node_idx_along_path_b,
        a.end_node_idx_along_path_b
      FROM cte_overlapping_node_sequences AS a
        LEFT OUTER JOIN cte_overlapping_node_sequences AS b
          ON (
            ( a.linear_id_a = b.linear_id_a )
            AND
            ( a.direction_a = b.direction_a )
            AND
            ( a.linear_id_b = b.linear_id_b )
            AND
            ( a.direction_b = b.direction_b )
            AND
            ( a.overlap_n < b.overlap_n )
            AND
            (
              a.start_node_idx_along_path_a
                BETWEEN b.start_node_idx_along_path_a AND b.end_node_idx_along_path_a
            )
          )
      WHERE (
        ( a.overlap_n > 0 )
        AND
        (
          -- ? Do these happen because of rounding coordinates ?
          --   They cause wrong directions to overlap, so they need to be excluded.
          NOT (
            ( a.overlap_n = 1 )
            AND
            ( a.start_node_id = a.end_node_id )
          )
        )
        AND
        ( b.linear_id_a IS NULL )
      )
), cte_collected AS (
    SELECT
        linear_id_a,
        direction_a,

        linear_id_b,
        direction_b,

        start_node_id,
        end_node_id,

        start_node_idx_along_path_a,
        end_node_idx_along_path_a,

        start_node_idx_along_path_b,
        end_node_idx_along_path_b

      FROM cte_max_overlapping_node_sequences

    UNION ALL

    SELECT
        linear_id_a,
        direction_a,

        linear_id_b,
        direction_b,

        a.start_node_id,
        b.end_node_id,

        a.start_node_idx_along_path_a,
        b.end_node_idx_along_path_a,

        a.start_node_idx_along_path_b,
        b.end_node_idx_along_path_b

      FROM cte_collected AS a
        INNER JOIN cte_max_overlapping_node_sequences AS b
          USING (linear_id_a, direction_a, linear_id_b, direction_b)
      WHERE (
        --  We loosen the restriction for inclusion here to accommodate
        --  single node differences between two TmcLinears.
        --
        --  FIXME: ? Is this still correct/required since fixing npmrds_network_paths_node_idx_:YEAR ?
        ( ( b.start_node_idx_along_path_a - a.end_node_idx_along_path_a ) BETWEEN 0 AND 1 )
        OR
        ( ( b.start_node_idx_along_path_b - a.end_node_idx_along_path_b ) BETWEEN 0 AND 1 )
      )
)
  INSERT INTO npmrds_network_spatial_analysis.npmrds_network_path_overlaps_:YEAR (
    linear_id_a,
    direction_a,

    linear_id_b,
    direction_b,

    start_node_id,
    end_node_id,

    start_node_idx_along_path_a,
    end_node_idx_along_path_a,

    start_node_idx_along_path_b,
    end_node_idx_along_path_b
  )
      --  Get the max overlaps out of cte_collected
      --
      --  FIXME FIXME FIXME:  Why are we getting duplicates here?
      --                      Key (12000408, NORTHBOUND, 12000489, EASTBOUND, 37963, 38171) already exists.
      SELECT DISTINCT ON (linear_id_a, direction_a, linear_id_b, direction_b, start_node_id, end_node_id)
          a.linear_id_a,
          a.direction_a,

          a.linear_id_b,
          a.direction_b,

          a.start_node_id,
          a.end_node_id,

          a.start_node_idx_along_path_a,
          a.end_node_idx_along_path_a,

          a.start_node_idx_along_path_b,
          a.end_node_idx_along_path_b

        FROM cte_collected AS a
          LEFT OUTER JOIN cte_collected AS b
            ON (
              ( a.linear_id_a = b.linear_id_a )
              AND
              ( a.direction_a = b.direction_a )
              AND
              ( a.linear_id_b = b.linear_id_b )
              AND
              ( a.direction_b = b.direction_b )
              AND
              (
                a.start_node_idx_along_path_a
                  BETWEEN b.start_node_idx_along_path_a AND b.end_node_idx_along_path_a
              )
              AND
              (
                ( a.end_node_idx_along_path_a - a.start_node_idx_along_path_a )
                <
                ( b.end_node_idx_along_path_a - b.start_node_idx_along_path_a )
              )
            )
      WHERE ( b.linear_id_a IS NULL )
      ORDER BY
        a.linear_id_a,
        a.direction_a,
        a.linear_id_b,
        a.direction_b,
        a.start_node_id,
        a.end_node_id,
        (a.end_node_idx_along_path_a - a.start_node_idx_along_path_a) DESC,
        (a.end_node_idx_along_path_b - a.start_node_idx_along_path_b) DESC
;

UPDATE npmrds_network_spatial_analysis.npmrds_network_path_overlaps_:YEAR AS a
  SET
      tmcs_a = b.tmcs_a,
      tmcs_b = b.tmcs_b
    FROM (
      -- FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME
      -- SEE: ISSUES.md "Self-overlapping TMC Linears causing issues in overlap detection"
      SELECT
          overlap_id,

          a.tmcs_a,
          b.tmcs_b

        FROM (
            SELECT
                overlap_id,
                array_agg( tmc ORDER BY min_path_idx ) AS tmcs_a

              FROM (
                SELECT
                    a.overlap_id,

                    b.tmc,
                    MIN(b.node_idx_along_path) AS min_path_idx

                  FROM npmrds_network_spatial_analysis.npmrds_network_path_overlaps_:YEAR AS a
                    INNER JOIN npmrds_network_spatial_analysis.npmrds_network_paths_node_idx_:YEAR AS b
                      ON (
                        ( a.linear_id_a = b.linear_id )
                        AND
                        ( a.direction_a = b.direction )
                        AND
                        (
                          b.node_idx_along_path
                            BETWEEN a.start_node_idx_along_path_a AND a.end_node_idx_along_path_a
                        )
                      )
                  GROUP BY 1, 2
              ) AS t
              GROUP BY 1
          ) AS a INNER JOIN (
            SELECT
                overlap_id,

                array_agg( tmc ORDER BY min_path_idx ) AS tmcs_b

              FROM (
                SELECT
                    a.overlap_id,

                    b.tmc,
                    MIN(b.node_idx_along_path) AS min_path_idx

                  FROM npmrds_network_spatial_analysis.npmrds_network_path_overlaps_:YEAR AS a
                    INNER JOIN npmrds_network_spatial_analysis.npmrds_network_paths_node_idx_:YEAR AS b
                      ON (
                        ( a.linear_id_b = b.linear_id )
                        AND
                        ( a.direction_b = b.direction )
                        AND
                        (
                          b.node_idx_along_path
                            BETWEEN a.start_node_idx_along_path_b AND a.end_node_idx_along_path_b
                        )
                      )

                  GROUP BY 1,2
              ) AS t
              GROUP BY 1
          ) AS b USING (overlap_id)
    ) AS b

  WHERE ( a.overlap_id = b.overlap_id )
;


WITH RECURSIVE cte_segments_a AS (
  SELECT
      overlap_id,

      jsonb_agg(
        jsonb_build_object(
          'node_id',          a.cur_node_id,
          'node_idx',         a.node_idx_along_path,
          'dist_from_prev',   COALESCE(
                                public.ST_Distance(
                                  public.GEOGRAPHY(b.wkb_geometry),
                                  public.GEOGRAPHY(c.wkb_geometry)
                                ),
                                0
                              )
        )
        ORDER BY node_idx_along_path
      ) AS nodes_path_a

    FROM (
      SELECT DISTINCT ON ( overlap_id, node_idx_along_path )
          overlap_id,

          LAG(b.node_id, 1) OVER (
            PARTITION BY
              a.linear_id_a,
              a.direction_a,
              a.start_node_idx_along_path_a,
              a.end_node_idx_along_path_a
            ORDER BY b.node_idx_along_path
          ) AS prev_node_id,

          b.node_id AS cur_node_id,

          b.node_idx_along_path

        FROM npmrds_network_spatial_analysis.npmrds_network_path_overlaps_:YEAR AS a
          INNER JOIN npmrds_network_spatial_analysis.npmrds_network_paths_node_idx_:YEAR AS b
            ON (
              ( a.linear_id_a = b.linear_id )
              AND
              ( a.direction_a = b.direction )
              AND
              ( 
                b.node_idx_along_path
                  BETWEEN a.start_node_idx_along_path_a AND a.end_node_idx_along_path_a
              )
            )

        ORDER BY overlap_id, node_idx_along_path
  ) AS a
    LEFT OUTER JOIN npmrds_network_spatial_analysis.npmrds_network_nodes_:YEAR AS b
      ON ( a.prev_node_id = b.node_id )
    INNER JOIN npmrds_network_spatial_analysis.npmrds_network_nodes_:YEAR AS c
      ON ( a.cur_node_id = c.node_id )

  WHERE (
    ( COALESCE(prev_node_id, -1) != cur_node_id )
  )

  GROUP BY overlap_id
), cte_segments_b AS (
  SELECT
      overlap_id,

      json_agg(
        jsonb_build_object(
          'node_id',          a.cur_node_id,
          'dist_from_prev',   COALESCE(
                                public.ST_Distance(
                                  public.GEOGRAPHY(b.wkb_geometry),
                                  public.GEOGRAPHY(c.wkb_geometry)
                                ),
                                0
                              )
        )
        ORDER BY node_idx_along_path
      ) AS nodes_path_b

    FROM (
      SELECT DISTINCT ON ( overlap_id, node_idx_along_path )
          overlap_id,

          LAG(b.node_id, 1) OVER (
            PARTITION BY
              a.linear_id_b,
              a.direction_b,
              a.start_node_idx_along_path_b,
              a.end_node_idx_along_path_b
            ORDER BY b.node_idx_along_path
          ) AS prev_node_id,

          b.node_id AS cur_node_id,

          b.node_idx_along_path

        FROM npmrds_network_spatial_analysis.npmrds_network_path_overlaps_:YEAR AS a
          INNER JOIN npmrds_network_spatial_analysis.npmrds_network_paths_node_idx_:YEAR AS b
            ON (
              ( a.linear_id_b = b.linear_id )
              AND
              ( a.direction_b = b.direction )
              AND
              ( 
                b.node_idx_along_path
                  BETWEEN a.start_node_idx_along_path_b AND a.end_node_idx_along_path_b
              )
            )

        ORDER BY overlap_id, node_idx_along_path
  ) AS a
    LEFT OUTER JOIN npmrds_network_spatial_analysis.npmrds_network_nodes_:YEAR AS b
      ON ( a.prev_node_id = b.node_id )
    INNER JOIN npmrds_network_spatial_analysis.npmrds_network_nodes_:YEAR AS c
      ON ( a.cur_node_id = c.node_id )

  WHERE (
    ( COALESCE(prev_node_id, -1) != cur_node_id )
  )

  GROUP BY overlap_id
), cte_merged AS (
  SELECT
      overlap_id,

      nodes_path_a,
      nodes_path_b,

      0::INTEGER AS path_idx_a,
      0::DOUBLE PRECISION AS dist_from_prev_a,

      0::INTEGER AS path_idx_b,
      0::DOUBLE PRECISION AS dist_from_prev_b,

      ARRAY[(nodes_path_a->0->>'node_id')::INTEGER] AS merged_nodes_arr

    FROM cte_segments_a AS a
      INNER JOIN cte_segments_b AS b
        USING (overlap_id)

  UNION ALL

  SELECT
      overlap_id,

      nodes_path_a,
      nodes_path_b,

      CASE
        WHEN ( peek_node_id_a = peek_node_id_b )
          THEN path_idx_a + 1
        WHEN ( peek_dist_from_prev_a <= peek_dist_from_prev_b )
          THEN path_idx_a + 1
        ELSE path_idx_a
      END AS path_idx_a,

      CASE
        WHEN ( peek_node_id_a = peek_node_id_b )
          THEN 0
        WHEN ( peek_dist_from_prev_a <= peek_dist_from_prev_b )
          THEN peek_dist_from_prev_a
        ELSE dist_from_prev_a
      END AS dist_from_prev_a,

      CASE
        WHEN ( peek_node_id_a = peek_node_id_b )
          THEN path_idx_b + 1
        WHEN ( peek_dist_from_prev_a > peek_dist_from_prev_b )
          THEN path_idx_b + 1
        ELSE path_idx_b
      END AS path_idx_b,

      CASE
        WHEN ( peek_node_id_a = peek_node_id_b )
          THEN 0
        WHEN ( peek_dist_from_prev_a > peek_dist_from_prev_b )
          THEN peek_dist_from_prev_b
        ELSE dist_from_prev_b
      END AS dist_from_prev_b,

      CASE
        WHEN ( peek_node_id_a = peek_node_id_b )
          THEN array_append(merged_nodes_arr, peek_node_id_a)
        WHEN ( peek_dist_from_prev_a <= peek_dist_from_prev_b )
          THEN array_append(merged_nodes_arr, peek_node_id_a)
        ELSE array_append(merged_nodes_arr, peek_node_id_b)
      END AS merged_nodes_arr

    FROM (
      SELECT
          overlap_id,

          nodes_path_a,

          nodes_path_b,
          
          path_idx_a,
          dist_from_prev_a,

          path_idx_b,
          dist_from_prev_b,

          merged_nodes_arr,

          ( nodes_path_a->(path_idx_a + 1)->>'node_id' )::INTEGER AS peek_node_id_a,


          --  FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME
          --    Why do we need to handle the pointer overflow case?
          --    Is it because the end node is a duplicte for one of the paths?
          COALESCE(
            (
              dist_from_prev_a
              + 
              ( nodes_path_a->(path_idx_a + 1)->>'dist_from_prev' )::DOUBLE PRECISION
            ),
            1E+308
          ) AS peek_dist_from_prev_a,

          ( nodes_path_b->(path_idx_b + 1)->>'node_id' )::INTEGER AS peek_node_id_b,

          COALESCE(
            (
              dist_from_prev_b
              + 
              ( nodes_path_b->(path_idx_b + 1)->>'dist_from_prev' )::DOUBLE PRECISION
            ),
            1E+308
          ) AS peek_dist_from_prev_b

        FROM cte_merged
    ) AS t

    WHERE ( COALESCE(peek_node_id_a, peek_node_id_b) IS NOT NULL )
)
  UPDATE npmrds_network_spatial_analysis.npmrds_network_path_overlaps_:YEAR AS a
    SET   node_ids = b.node_ids
    FROM (
      SELECT DISTINCT ON ( overlap_id )
          overlap_id,

          merged_nodes_arr AS node_ids
          
        FROM cte_merged

        ORDER BY overlap_id, ARRAY_LENGTH(merged_nodes_arr, 1) DESC
    ) AS b
    WHERE ( a.overlap_id = b.overlap_id )
;

CREATE VIEW npmrds_network_spatial_analysis.overlaps_tmc_arrs_exploded_:YEAR
  AS
    SELECT
        overlap_id,
        a.linear_id_a AS linear_id,
        t.tmc,
        t.idx 
      FROM npmrds_network_spatial_analysis.npmrds_network_path_overlaps_:YEAR AS a,
        UNNEST(tmcs_a) WITH ORDINALITY AS t(tmc, idx)
    UNION ALL
      SELECT
        overlap_id,
        a.linear_id_b AS linear_id,
        t.tmc,
        t.idx 
      FROM npmrds_network_spatial_analysis.npmrds_network_path_overlaps_:YEAR AS a,
        UNNEST(tmcs_b) WITH ORDINALITY AS t(tmc, idx)
;

CREATE VIEW npmrds_network_spatial_analysis.overlaps_nodes_arr_exploded_:YEAR
  AS
    SELECT
        overlap_id,

        a.linear_id_a,
        a.linear_id_b,

        t.node_id,
        t.idx,

        ( a.start_node_id = t.node_id ) AS is_start_node,
        ( a.end_node_id = t.node_id ) AS is_end_node

      FROM npmrds_network_spatial_analysis.npmrds_network_path_overlaps_:YEAR AS a,
        UNNEST(node_ids) WITH ORDINALITY AS t(node_id, idx)
;

CLUSTER npmrds_network_spatial_analysis.npmrds_network_path_overlaps_:YEAR
  USING npmrds_network_path_overlaps_:YEAR_pkey
;
