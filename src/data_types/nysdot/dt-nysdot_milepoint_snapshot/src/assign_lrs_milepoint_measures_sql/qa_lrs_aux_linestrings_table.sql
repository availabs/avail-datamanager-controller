DROP TABLE IF EXISTS :ETL_WORK_SCHEMA.qa_lrs_aux_linestrings_calculated_geom_from_to_mi_01 CASCADE ;

CREATE TABLE :ETL_WORK_SCHEMA.qa_lrs_aux_linestrings_calculated_geom_from_to_mi_01
  AS
    SELECT
        y.*,
        ( lrs_aux_geom_from_mi_passes AND lrs_aux_geom_to_mi_passes ) AS passes
      FROM (
        SELECT
            x.*,
            ( ABS(lrs_aux_from_measure_diff_from_mi_feet) <= 10 ) AS lrs_aux_geom_from_mi_passes,
            ( ABS(lrs_aux_from_measure_diff_to_mi_feet) <= 10 ) AS lrs_aux_geom_to_mi_passes
          FROM (
            SELECT
                lrs_aux_lstr_id,

                lrs_aux_table_name,
                lrs_aux_ogc_fid,

                lrs_mpt_ogc_fid,
                route_id,

                lrs_aux_geom_from_to_diff,
                from_lt_to_measure,

                consistent_from_date,

                lrs_aux_lstr_idx,
                lrs_aux_lstr_n,

                lrs_aux_geom_from_measure,
                lrs_aux_geom_to_measure,

                lrs_aux_geom_len_mi,
                lrs_aux_lstr_len_mi,

                lrs_aux_geom_from_mi,
                lrs_aux_geom_to_mi,

                lrs_aux_lstr_from_mi,
                lrs_aux_lstr_to_mi,

                has_reliable_geom_from_to_measures,

                CASE
                  WHEN (lrs_aux_lstr_idx = 1) THEN
                    (
                      ( lrs_aux_geom_from_measure - lrs_aux_geom_from_mi )
                      * 5280
                    )
                  ELSE NULL
                END AS lrs_aux_from_measure_diff_from_mi_feet,

                CASE
                  WHEN (lrs_aux_lstr_idx = lrs_aux_lstr_n) THEN
                    (
                      ( lrs_aux_geom_to_measure - lrs_aux_geom_to_mi )
                      * 5280
                    )
                  ELSE NULL
                END AS lrs_aux_from_measure_diff_to_mi_feet

              FROM :ETL_WORK_SCHEMA.lrs_aux_linestrings
          ) AS x
    ) AS y
;

-- DROP TABLE IF EXISTS :ETL_WORK_SCHEMA.qa_lrs_aux_linestrings_calculated_geom_from_to_mi_02 CASCADE ;
--
-- --  Cospatial and lrs_aux_geom_from_to_mi_ranges_overlap MUST be biconditionals.
-- --    cospatial <=> lrs_aux_geom_from_to_mi_ranges_overlap
--
-- CREATE TABLE :ETL_WORK_SCHEMA.qa_lrs_aux_linestrings_calculated_geom_from_to_mi_02
--   AS
--     SELECT
--         lrs_mpt_ogc_fid,
--
--         BOOLEAN_AND(cospatial)
--           FILTER ( WHERE lrs_aux_geom_from_to_mi_ranges_overlap )
--           AS cospatial_consistent_with_from_to_mi_ranges,
--
--         BOOLEAN_AND(lrs_aux_geom_from_to_mi_ranges_overlap)
--           FILTER ( WHERE cospatial )
--           AS from_to_mi_ranges_consistent_with_cospatial,
--
--         jsong_agg(
--           json_build_object(
--             'lrs_aux_lstrs',
--             jsonb_build_array(
--               json_build_object(
--                 'lrs_aux_table_name',   lrs_aux_table_name_a,
--                 'lrs_aux_ogc_fid',      lrs_aux_ogc_fid_a,
--                 'lrs_aux_lstr_id',      lrs_aux_lstr_id_a
--               ),
--
--               json_build_object(
--                 'lrs_aux_table_name',   lrs_aux_table_name_b,
--                 'lrs_aux_ogc_fid',      lrs_aux_ogc_fid_b,
--                 'lrs_aux_lstr_id',      lrs_aux_lstr_id_b
--               ),
--             ),
--
--             'meta',
--             json_build_object(
--               'cospatial',                                cospatial,
--               'lrs_aux_geom_from_to_mi_ranges_overlap',   lrs_aux_geom_from_to_mi_ranges_overlap,
--             )
--           )
--         ) FILTER (
--           WHERE (
--             ( cospatial AND NOT lrs_aux_geom_from_to_mi_ranges_overlap )
--             OR
--             ( lrs_aux_geom_from_to_mi_ranges_overlap AND NOT cospatial )
--           )
--         ) AS inconisistent_lrs_aux_lstr_pairs
--
--       FROM (
--         SELECT
--             lrs_mpt_ogc_fid,
--
--             a.lrs_aux_table_name AS lrs_aux_table_name_a,
--             a.lrs_aux_ogc_fid AS lrs_aux_ogc_fid_a,
--             a.lrs_aux_lstr_id AS lrs_aux_lstr_id_a,
--
--             b.lrs_aux_table_name AS lrs_aux_table_name_b,
--             b.lrs_aux_ogc_fid AS lrs_aux_ogc_fid_b,
--             b.lrs_aux_lstr_id AS lrs_aux_lstr_id_b,
--
--             (
--               ST_Intersects(a.wkb_geometry, b.wkb_geometry)
--               AND
--               NOT ST_Touches(a.wkb_geometry, b.wkb_geometry)
--             ) AS are_cospatial,
--
--             (
--               numrange(
--                 LEAST(a.lrs_aux_geom_from_mi::NUMERIC, a.lrs_aux_geom_to_mi::NUMERIC, '[]'),
--                 GREATEST(a.lrs_aux_geom_from_mi::NUMERIC, a.lrs_aux_geom_to_mi::NUMERIC, '[]')
--               )
--               &&
--               numrange(
--                 LEAST(b.lrs_aux_geom_from_mi::NUMERIC, b.lrs_aux_geom_to_mi::NUMERIC, '[]'),
--                 GREATEST(b.lrs_aux_geom_from_mi::NUMERIC, b.lrs_aux_geom_to_mi::NUMERIC, '[]')
--               )
--             ) AS lrs_aux_geom_from_to_mi_ranges_overlap
--
--           FROM :ETL_WORK_SCHEMA.lrs_aux_linestrings AS a
--             INNER JOIN :ETL_WORK_SCHEMA.lrs_aux_linestrings
--               USING (lrs_mpt_ogc_fid)
--
--           WHERE ( a.lrs_aux_lstr_id < b.lrs_aux_lstr_id )
--       ) AS x
-- ;
--
--
-- DO $$
--   BEGIN
--       RAISE NOTICE '
--
-- ==> Created VIEW qa_lrs_aux_lstr_from_to_mi_test;
--       ' ;
--
-- END ; $$
