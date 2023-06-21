/*
  NOTE: Could have ALL labels as ARRAY and append to LABELS reinforcing matches.

  TODO: Create VIEWs for the TMC Nodes and Overlap Nodes. INSERT/UPDATE using those VIEWS.
        Keep the VIEWs for observability.
*/

BEGIN ;

DROP VIEW IF EXISTS npmrds_network_spatial_analysis.npmrds_network_matched_tmc_nodes_:YEAR_A_:YEAR_B ;

CREATE VIEW npmrds_network_spatial_analysis.npmrds_network_matched_tmc_nodes_:YEAR_A_:YEAR_B
  AS
    SELECT
        node_id_a,
        node_id_b,
        label,
        label_fields,
        '0.001' AS conformal_level,
        'MATCHED_TMC_NODE' AS match_class
      FROM (
        SELECT
            *
          FROM (
            SELECT DISTINCT ON (node_id_a, node_id_b)
                a.start_node_id AS node_id_a,
                b.start_node_id AS node_id_b,

                jsonb_build_object(
                  'tmc',                    tmc,
                  'pt_diff_meters',         start_pt_diff_meters,
                  'location',               'start'
                ) AS label,

                jsonb_build_array('tmc', 'pt_diff_meters', 'location') AS label_fields

              FROM npmrds_network_spatial_analysis.npmrds_tmc_similarity_:YEAR_A_:YEAR_B AS x
                INNER JOIN npmrds_network_spatial_analysis.npmrds_network_edge_metadata_:YEAR_A AS a
                  USING (tmc)
                INNER JOIN npmrds_network_spatial_analysis.npmrds_network_edge_metadata_:YEAR_B AS b
                  USING (tmc)
              WHERE ( x.start_pt_diff_meters <= 1.5 AND x.start_pt_diff_meters < 1.5)
              ORDER BY node_id_a, node_id_b, x.start_pt_diff_meters, x.end_pt_diff_meters
          ) AS t1

        UNION 

        SELECT
            *
          FROM (
            SELECT DISTINCT ON (node_id_a, node_id_b)
                a.start_node_id AS node_id_a,
                b.start_node_id AS node_id_b,

                jsonb_build_object(
                  'tmc',                    tmc,
                  'pt_diff_meters',         end_pt_diff_meters,
                  'location',               'end'
                ) AS label,

                jsonb_build_array('tmc', 'pt_diff_meters', 'location') AS label_fields

              FROM npmrds_network_spatial_analysis.npmrds_tmc_similarity_:YEAR_A_:YEAR_B AS x
                INNER JOIN npmrds_network_spatial_analysis.npmrds_network_edge_metadata_:YEAR_A AS a
                  USING (tmc)
                INNER JOIN npmrds_network_spatial_analysis.npmrds_network_edge_metadata_:YEAR_B AS b
                  USING (tmc)
              WHERE ( x.start_pt_diff_meters <= 1.5 AND x.start_pt_diff_meters < 1.5)
              ORDER BY node_id_a, node_id_b, x.start_pt_diff_meters, x.end_pt_diff_meters
          ) AS t2
      ) AS t
;

INSERT INTO npmrds_network_spatial_analysis.npmrds_network_conformal_matches_:YEAR_A_:YEAR_B (
  node_id_a,
  node_id_b,
  label,
  label_fields,
  conformal_level,
  match_class
)
  SELECT
      node_id_a,
      node_id_b,
      label,
      label_fields,
      '0.001' AS conformal_level,
      'MATCHED_TMC_NODE' AS match_class
    FROM npmrds_network_spatial_analysis.npmrds_network_matched_tmc_nodes_:YEAR_A_:YEAR_B
  ON CONFLICT DO NOTHING
; 

--  WITH cte_matched_tmclinear_merge_splits AS (
DROP VIEW IF EXISTS npmrds_network_spatial_analysis.npmrds_network_path_overlap_merges_and_splits_:YEAR_A_:YEAR_B ;

CREATE VIEW npmrds_network_spatial_analysis.npmrds_network_path_overlap_merges_and_splits_:YEAR_A_:YEAR_B AS
  WITH cte_overlap_node_matches AS (
    SELECT
        *
      FROM (
        SELECT
            start_node_id_a,
            start_node_id_b,

            end_node_id_a,
            end_node_id_b,

            jsonb_build_object(
              'linear_id_a',            linear_id_a,
              'direction_a',            direction_a,
              'linear_id_b',            linear_id_b,
              'direction_b',            direction_b,
              'pt_diff_meters',         start_pt_diff_meters,
              'location',               'merge'
            ) AS start_pt_label,

            jsonb_build_object(
              'linear_id_a',            linear_id_a,
              'direction_a',            direction_a,
              'linear_id_b',            linear_id_b,
              'direction_b',            direction_b,
              'pt_diff_meters',         end_pt_diff_meters,
              'location',               'split'
            ) AS end_pt_label,

            jsonb_build_array(
              'linear_id_a',
              'direction_a',
              'linear_id_b',
              'direction_b',
              'pt_diff_meters',
              'location'
            ) AS label_fields

          FROM (
            SELECT
                linear_id_a,
                direction_a,

                linear_id_b,
                direction_b,

                a.start_node_id AS start_node_id_a,
                b.start_node_id AS start_node_id_b,

                a.end_node_id AS end_node_id_a,
                b.end_node_id AS end_node_id_b,

                ST_Distance(
                  GEOGRAPHY(c.wkb_geometry),
                  GEOGRAPHY(e.wkb_geometry)
                ) AS start_pt_diff_meters,

                ST_Distance(
                  GEOGRAPHY(d.wkb_geometry),
                  GEOGRAPHY(f.wkb_geometry)
                ) AS end_pt_diff_meters

              FROM npmrds_network_spatial_analysis.npmrds_network_path_overlaps_:YEAR_A AS a
                INNER JOIN npmrds_network_spatial_analysis.npmrds_network_path_overlaps_:YEAR_B AS b
                  USING (linear_id_a, direction_a, linear_id_b, direction_b)
                INNER JOIN npmrds_network_spatial_analysis.npmrds_network_nodes_:YEAR_A AS c
                  ON ( a.start_node_id = c.node_id )
                INNER JOIN npmrds_network_spatial_analysis.npmrds_network_nodes_:YEAR_A AS d
                  ON ( a.end_node_id = d.node_id )
                INNER JOIN npmrds_network_spatial_analysis.npmrds_network_nodes_:YEAR_B AS e
                  ON ( b.start_node_id = e.node_id )
                INNER JOIN npmrds_network_spatial_analysis.npmrds_network_nodes_:YEAR_B AS f
                  ON ( b.end_node_id = f.node_id )

              WHERE (
                (
                  ( ( a.end_node_idx_along_path_a - a.start_node_idx_along_path_a ) > 2 )
                  AND
                  ( ( a.end_node_idx_along_path_b - a.start_node_idx_along_path_b ) > 2 )
                )
                AND
                (
                  ( ( b.end_node_idx_along_path_a - b.start_node_idx_along_path_a ) > 2 )
                  AND
                  ( ( b.end_node_idx_along_path_b - b.start_node_idx_along_path_b ) > 2 )
                )
              )

          ) AS t

          WHERE (
           ( start_pt_diff_meters <= ( :PT_DIFF_METERS * 2 ) )
           AND
           ( end_pt_diff_meters   <= ( :PT_DIFF_METERS * 2 ) )
          )

          ORDER BY start_pt_diff_meters, end_pt_diff_meters
      ) AS t
  )
    SELECT
        *
      FROM (
        SELECT
            *
          FROM (
            SELECT DISTINCT ON (node_id_a, node_id_b)
                start_node_id_a AS node_id_a,
                start_node_id_b AS node_id_b,
                start_pt_label AS label,
                label_fields,
                '0.001' AS conformal_level,
                'TMC_LINEAR_PATHS_MERGE_NODE' AS match_class
              FROM cte_overlap_node_matches
              WHERE ( (start_pt_label->>'pt_diff_meters')::REAL < :PT_DIFF_METERS )
              ORDER BY
                  node_id_a,
                  node_id_b,
                  (start_pt_label->>'pt_diff_meters')::REAL,
                  (start_pt_label->>'linear_id_a')::INTEGER, 
                  (start_pt_label->>'linear_id_b')::INTEGER,
                  (start_pt_label->>'location')::TEXT
          ) AS a

        UNION ALL

        SELECT
            *
          FROM (
            SELECT DISTINCT ON (node_id_a, node_id_b)
                end_node_id_a AS node_id_a,
                end_node_id_b AS node_id_b,
                end_pt_label AS label,
                label_fields,
                '0.001' AS conformal_level,
                'TMC_LINEAR_PATHS_SPLIT_NODE' AS match_class
              FROM cte_overlap_node_matches
              WHERE ( (end_pt_label->>'pt_diff_meters')::REAL < :PT_DIFF_METERS )
              ORDER BY
                  node_id_a,
                  node_id_b,
                  (end_pt_label->>'pt_diff_meters')::REAL,
                  (end_pt_label->>'linear_id_a')::INTEGER, 
                  (end_pt_label->>'linear_id_b')::INTEGER,
                  (end_pt_label->>'location')::TEXT
          ) AS b

      ) AS t
; 

INSERT INTO npmrds_network_spatial_analysis.npmrds_network_conformal_matches_:YEAR_A_:YEAR_B (
  node_id_a,
  node_id_b,
  label,
  label_fields,
  conformal_level,
  match_class
)
  SELECT DISTINCT ON (node_id_a, node_id_b)
      node_id_a,
      node_id_b,
      label,
      label_fields,
      conformal_level,
      match_class
    FROM npmrds_network_spatial_analysis.npmrds_network_path_overlap_merges_and_splits_:YEAR_A_:YEAR_B
    ORDER BY node_id_a, node_id_b, match_class /* prefer MERGE to SPLIT */
  ON CONFLICT DO NOTHING
;

UPDATE npmrds_network_spatial_analysis.npmrds_network_conformal_matches_:YEAR_A_:YEAR_B AS a
  SET match_class = b.match_class

  FROM (
    SELECT DISTINCT ON (node_id_a, node_id_b)
        node_id_a,
        node_id_b,
        match_class
      FROM npmrds_network_spatial_analysis.npmrds_network_path_overlap_merges_and_splits_:YEAR_A_:YEAR_B
      ORDER BY node_id_a, node_id_b, match_class /* prefer MERGE to SPLIT */
  ) AS b
  WHERE (
    ( a.node_id_a = b.node_id_a )
    AND
    ( a.node_id_b = b.node_id_b )
    AND
    ( a.match_class IS NULL )
  )
;

DROP VIEW IF EXISTS npmrds_network_spatial_analysis.npmrds_network_path_matches_internal_:YEAR_A_:YEAR_B CASCADE ;
CREATE VIEW npmrds_network_spatial_analysis.npmrds_network_path_matches_internal_:YEAR_A_:YEAR_B
  AS
    WITH cte_foo AS (
      SELECT
          a.node_id_a,
          a.node_id_b,
          a.label,
          a.label_fields,

          linear_ids_a,
          linear_ids_b

          --  jsonb_agg(DISTINCT b.linear_id ORDER BY b.linear_id) AS linear_ids_a,
          --  jsonb_agg(DISTINCT c.linear_id ORDER BY c.linear_id) AS linear_ids_b

        FROM npmrds_network_spatial_analysis.npmrds_network_conformal_matches_:YEAR_A_:YEAR_B AS a
          INNER JOIN (
            SELECT
                node_id AS node_id_a,
                array_agg(DISTINCT linear_id ORDER BY linear_id) AS linear_ids_a
              FROM (
                SELECT
                    node_id,
                    linear_id_a AS linear_id
                  FROM npmrds_network_spatial_analysis.overlaps_nodes_arr_exploded_:YEAR_A 
                  WHERE ( NOT ( is_start_node OR is_end_node ) )
                UNION 
                SELECT
                    node_id,
                    linear_id_b AS linear_id
                  FROM npmrds_network_spatial_analysis.overlaps_nodes_arr_exploded_:YEAR_A 
                  WHERE ( NOT ( is_start_node OR is_end_node ) )
              ) AS t
              GROUP BY 1
          ) AS b USING (node_id_a)
          INNER JOIN (
            SELECT
                node_id AS node_id_b,
                array_agg(DISTINCT linear_id ORDER BY linear_id) AS linear_ids_b
              FROM (
                SELECT
                    node_id,
                    linear_id_a AS linear_id
                  FROM npmrds_network_spatial_analysis.overlaps_nodes_arr_exploded_:YEAR_B
                  WHERE ( NOT ( is_start_node OR is_end_node ) )
                UNION 
                SELECT
                    node_id,
                    linear_id_b AS linear_id
                  FROM npmrds_network_spatial_analysis.overlaps_nodes_arr_exploded_:YEAR_B
                  WHERE ( NOT ( is_start_node OR is_end_node ) )
              ) AS t
              GROUP BY 1
          ) AS c USING ( node_id_b )
    )
      SELECT
          a.*
        FROM cte_foo AS a
          INNER JOIN (
            SELECT
                node_id AS node_id_a,
                array_agg(DISTINCT linear_id::INTEGER ORDER BY linear_id::INTEGER) AS linear_ids_a
              FROM npmrds_network_spatial_analysis.npmrds_network_node_incident_edges_metadata_:YEAR_A
              GROUP BY 1
            INTERSECT
            SELECT
                node_id_a,
                linear_ids_a
              FROM cte_foo
          ) AS b USING (node_id_a, linear_ids_a)
          INNER JOIN (
            SELECT
                node_id AS node_id_b,
                array_agg(DISTINCT linear_id::INTEGER ORDER BY linear_id::INTEGER) AS linear_ids_b
              FROM npmrds_network_spatial_analysis.npmrds_network_node_incident_edges_metadata_:YEAR_B
              GROUP BY 1
            INTERSECT
            SELECT
                node_id_b,
                linear_ids_b
              FROM cte_foo
          ) AS c USING (node_id_b, linear_ids_b)
;

-- -- FIXME: Too many false positives for unmatched/nonintersection pairings.
--           The unmatched include a lot of overlap intersections. Need to filter them out.
--           Also lots of errors where multi-carriage ways merge and split.
--           Also, these are a bit less reliable. Intersections are macro-level. Coords are micro-level.
--
-- CREATE TEMPORARY TABLE tmp_:YEAR_A_unmatched_intersection_nodes (
--     node_id_a     INTEGER PRIMARY KEY,
--     wkb_geometry  public.geometry(Point, 4326)
--   ) WITH (fillfactor=100)
-- ;
-- 
-- CREATE INDEX tmp_:YEAR_A_unmatched_intersection_nodes_gidx
--   ON tmp_:YEAR_A_unmatched_intersection_nodes USING GIST(GEOGRAPHY(wkb_geometry))
-- ;
-- 
-- INSERT INTO tmp_:YEAR_A_unmatched_intersection_nodes
--   SELECT DISTINCT
--       a.node_id AS node_id_a,
--       a.wkb_geometry
--     FROM npmrds_network_spatial_analysis.npmrds_network_node_incident_edges_metadata_:YEAR_A AS a
--       LEFT OUTER JOIN npmrds_network_spatial_analysis.npmrds_network_conformal_matches_:YEAR_A_:YEAR_B AS b
--         ON ( a.node_id = b.node_id_a )
--     WHERE ( b.node_id_a IS NULL )
-- ;
-- 
-- CLUSTER tmp_:YEAR_A_unmatched_intersection_nodes
--   USING tmp_:YEAR_A_unmatched_intersection_nodes_gidx
-- ;
-- 
-- CREATE TEMPORARY TABLE tmp_:YEAR_B_unmatched_intersection_nodes (
--     node_id_b     INTEGER PRIMARY KEY,
--     wkb_geometry  public.geometry(Point, 4326)
--   ) WITH (fillfactor=100)
-- ;
-- 
-- CREATE INDEX tmp_:YEAR_B_unmatched_intersection_nodes_gidx
--   ON tmp_:YEAR_B_unmatched_intersection_nodes USING GIST(GEOGRAPHY(wkb_geometry))
-- ;
-- 
-- INSERT INTO tmp_:YEAR_B_unmatched_intersection_nodes
--   SELECT DISTINCT
--       a.node_id AS node_id_b,
--       a.wkb_geometry
--     FROM npmrds_network_spatial_analysis.npmrds_network_node_incident_edges_metadata_:YEAR_B AS a
--       LEFT OUTER JOIN npmrds_network_spatial_analysis.npmrds_network_conformal_matches_:YEAR_A_:YEAR_B AS b
--         ON ( a.node_id = b.node_id_b )
--     WHERE ( b.node_id_b IS NULL )
-- ;
-- 
-- CLUSTER tmp_:YEAR_B_unmatched_intersection_nodes
--   USING tmp_:YEAR_B_unmatched_intersection_nodes_gidx
-- ;
-- 
-- CREATE TEMPORARY TABLE tmp_:YEAR_A_nonintersection_nodes (
--     node_id_a     INTEGER PRIMARY KEY,
--     wkb_geometry  public.geometry(Point, 4326)
--   ) WITH (fillfactor=100)
-- ;
-- 
-- CREATE INDEX tmp_:YEAR_A_nonintersection_nodes_gidx
--   ON tmp_:YEAR_A_nonintersection_nodes USING GIST(GEOGRAPHY(wkb_geometry))
-- ;
-- 
-- INSERT INTO tmp_:YEAR_A_nonintersection_nodes
--   SELECT DISTINCT
--       a.node_id AS node_id_a,
--       a.wkb_geometry
--     FROM npmrds_network_spatial_analysis.npmrds_network_nodes_:YEAR_A AS a
--       LEFT OUTER JOIN npmrds_network_spatial_analysis.npmrds_network_node_incident_edges_metadata_:YEAR_A AS b
--         USING (node_id)
--     WHERE ( b.node_id IS NULL )
-- ;
-- 
-- CLUSTER tmp_:YEAR_A_nonintersection_nodes
--   USING tmp_:YEAR_A_nonintersection_nodes_gidx
-- ;
-- 
-- CREATE TEMPORARY TABLE tmp_:YEAR_B_nonintersection_nodes (
--     node_id_b     INTEGER PRIMARY KEY,
--     wkb_geometry  public.geometry(Point, 4326)
--   ) WITH (fillfactor=100)
-- ;
-- 
-- CREATE INDEX tmp_:YEAR_B_nonintersection_nodes_gidx
--   ON tmp_:YEAR_B_nonintersection_nodes USING GIST(GEOGRAPHY(wkb_geometry))
-- ;
-- 
-- INSERT INTO tmp_:YEAR_B_nonintersection_nodes
--   SELECT DISTINCT
--       a.node_id AS node_id_b,
--       a.wkb_geometry
--     FROM npmrds_network_spatial_analysis.npmrds_network_nodes_:YEAR_B AS a
--       LEFT OUTER JOIN npmrds_network_spatial_analysis.npmrds_network_node_incident_edges_metadata_:YEAR_B AS b
--         USING (node_id)
--     WHERE ( b.node_id IS NULL )
-- ;
-- 
-- CLUSTER tmp_:YEAR_B_nonintersection_nodes
--   USING tmp_:YEAR_B_nonintersection_nodes_gidx
-- ;


-- DROP TABLE IF EXISTS npmrds_network_spatial_analysis.tmp_foo CASCADE ;
-- CREATE TABLE npmrds_network_spatial_analysis.tmp_foo
--   AS
--       SELECT
--           node_id_a,
--           node_id_b,
--           'UNMATCHED_A_TO_NONINTERSECTION_B' AS match_class
--         FROM tmp_:YEAR_A_unmatched_intersection_nodes AS a
--           INNER JOIN tmp_:YEAR_B_nonintersection_nodes AS b
--             ON (
--               public.ST_Distance(
--                 GEOGRAPHY(a.wkb_geometry),
--                 GEOGRAPHY(b.wkb_geometry)
--               ) <= ( 1.5 ) /*meter*/
--             )
--       UNION
--       SELECT
--           node_id_a,
--           node_id_b,
--           'UNMATCHED_B_TO_NONINTERSECTION_A' AS match_class
--         FROM tmp_:YEAR_A_nonintersection_nodes AS a
--           INNER JOIN tmp_:YEAR_B_unmatched_intersection_nodes AS b
--             ON (
--               public.ST_Distance(
--                 GEOGRAPHY(a.wkb_geometry),
--                 GEOGRAPHY(b.wkb_geometry)
--               ) <= ( 1.5 ) /*meter*/
--             )
-- ;
-- 
-- DROP TABLE IF EXISTS npmrds_network_spatial_analysis.tmp_foo CASCADE ;
-- CREATE TABLE npmrds_network_spatial_analysis.tmp_foo
--   AS
--     SELECT DISTINCT ON (node_id_a, node_id_b)
--         node_id_a,
--         node_id_b,
--         dist,
--         match_class
--       FROM (
--         SELECT
--             node_id_a,
--             node_id_b,
--             dist,
--             'UNMATCHED_A_TO_NONINTERSECTION_B' AS match_class
--           FROM (
--             SELECT
--                 node_id_a,
--                 node_id_b,
--                 b.dist
--               FROM tmp_:YEAR_A_unmatched_intersection_nodes AS a
--                 CROSS JOIN LATERAL (
--                   SELECT
--                       node_id_b,
--                       (GEOGRAPHY(a.wkb_geometry) <-> GEOGRAPHY(x.wkb_geometry)) AS dist
--                     FROM tmp_:YEAR_B_nonintersection_nodes AS x
--                     ORDER BY DIST
--                     LIMIT 1
--                 ) AS b
--           ) AS t
--         UNION ALL
--         SELECT
--             node_id_a,
--             node_id_b,
--             dist,
--             'UNMATCHED_B_TO_NONINTERSECTION_A' AS match_class
--           FROM (
--             SELECT
--                 node_id_a,
--                 node_id_b,
--                 b.dist
--               FROM tmp_:YEAR_B_unmatched_intersection_nodes AS a
--                 CROSS JOIN LATERAL (
--                   SELECT
--                       node_id_a,
--                       (GEOGRAPHY(a.wkb_geometry) <-> GEOGRAPHY(x.wkb_geometry)) AS dist
--                     FROM tmp_:YEAR_A_nonintersection_nodes AS x
--                     ORDER BY DIST
--                     LIMIT 1
--                 ) AS b
--           ) AS t
--     ) AS t
--     WHERE ( dist < 3 )
-- ;
-- 
-- INSERT INTO npmrds_network_spatial_analysis.npmrds_network_conformal_matches_:YEAR_A_:YEAR_B (
--   node_id_a,
--   node_id_b,
--   label,
--   label_fields,
--   conformal_level,
--   match_class
-- )
--   SELECT
--       node_id_a,
--       node_id_b,
--       jsonb_build_array(dist)  AS label,
--       jsonb_build_array('distance') AS label_fields,
--       '7.001' AS conformal_level,
--       match_class
--     FROM npmrds_network_spatial_analysis.tmp_foo
--   ON CONFLICT DO NOTHING
-- ; 

-- NOTE:  This is done at the end because TMC_LINEAR_PATHS_OVERLAP_INTERNAL_NODES are filtered out of the result.
--          They may later be deleted. I haven't decided to go that far with it yet, however.
--         are a MASSIVE source of false positives.
--        If you want to keep a match, assign a match_class BEFORE this runs.
UPDATE npmrds_network_spatial_analysis.npmrds_network_conformal_matches_:YEAR_A_:YEAR_B AS a
  SET match_class = 'TMC_LINEAR_PATHS_OVERLAP_INTERNAL_NODES'

  FROM (
    SELECT DISTINCT ON (node_id_a, node_id_b)
        node_id_a,
        node_id_b
      FROM npmrds_network_spatial_analysis.npmrds_network_path_matches_internal_:YEAR_A_:YEAR_B
  ) AS b
  WHERE (
    ( a.node_id_a = b.node_id_a )
    AND
    ( a.node_id_b = b.node_id_b )
    AND
    ( a.match_class IS NULL )
  )
;

CLUSTER npmrds_network_spatial_analysis.npmrds_network_conformal_matches_:YEAR_A_:YEAR_B
  USING npmrds_network_conformal_matches_:YEAR_A_:YEAR_B_pkey
;

COMMIT ;
