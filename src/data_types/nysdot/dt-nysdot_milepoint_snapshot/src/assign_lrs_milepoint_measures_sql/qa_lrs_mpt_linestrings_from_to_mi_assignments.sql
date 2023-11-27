DROP TABLE IF EXISTS :ETL_WORK_SCHEMA.qa_lrs_milepoint_linestrings_mi_assignments CASCADE ;

CREATE TABLE :ETL_WORK_SCHEMA.qa_lrs_milepoint_linestrings_mi_assignments
  AS
    SELECT
        ROW_NUMBER() OVER () AS lsr_mpt_aux_lstr_pair_id,

        y.*,
        (
          (mi_ranges_overlap AND lrs_mpt_and_aux_lstrs_are_cospatial)
          OR
          NOT (
            ( mi_ranges_overlap_ft > 3 )
            OR
            lrs_mpt_and_aux_lstrs_are_cospatial
          )
        ) AS passes
      FROM (
        SELECT
            x.*,

            (
             lrs_milepoint_lstr_exclusive_assigned_measure_range
             &&
             lrs_aux_geom_mi_exclusive_range
            ) AS mi_ranges_overlap,

            (
             lrs_milepoint_lstr_exclusive_assigned_measure_range
             *
             lrs_aux_geom_mi_exclusive_range
            ) AS mi_ranges_intersection,


            (
              (
                upper((
                  lrs_milepoint_lstr_exclusive_assigned_measure_range
                  *
                  lrs_aux_geom_mi_exclusive_range
                ))
                -
                lower((
                  lrs_milepoint_lstr_exclusive_assigned_measure_range
                  *
                  lrs_aux_geom_mi_exclusive_range
                ))
              ) * 5280
            ) AS mi_ranges_overlap_ft

          FROM (
            SELECT
                lrs_mpt_ogc_fid,
                route_id,

                b.lrs_mpt_lstr_id,
                b.lrs_mpt_lstr_idx,
                b.lrs_mpt_lstr_n,

                a.assigned_from_measure AS lrs_mpt_lstr_assigned_from_measure,
                a.assigned_to_measure AS lrs_mpt_lstr_assigned_to_measure,

                numrange(
                  a.assigned_from_measure::NUMERIC,
                  a.assigned_to_measure::NUMERIC,
                  '()'
                ) AS lrs_milepoint_lstr_exclusive_assigned_measure_range,

                c.lrs_aux_table_name,
                c.lrs_aux_ogc_fid,
                c.lrs_aux_lstr_id,
                c.lrs_aux_lstr_idx,
                c.lrs_aux_lstr_n,

                c.lrs_aux_geom_from_measure,
                c.lrs_aux_geom_to_measure,
                c.lrs_aux_geom_len_mi,
                c.lrs_aux_lstr_len_mi,
                c.lrs_aux_geom_from_mi,
                c.lrs_aux_geom_to_mi,
                c.lrs_aux_lstr_from_mi,
                c.lrs_aux_lstr_to_mi,
                c.lrs_aux_geom_measure_exclusive_range,
                c.lrs_aux_geom_mi_exclusive_range,
                c.lrs_aux_lstr_mi_exclusive_range,

                ST_NumPoints(c.wkb_geometry) AS lrs_aux_lstr_num_points,

                (
                  ST_Intersects(b.wkb_geometry, c.wkb_geometry)
                  AND NOT ST_Touches(b.wkb_geometry, c.wkb_geometry)
                ) AS lrs_mpt_and_aux_lstrs_are_cospatial,


                b.wkb_geometry AS lrs_mpt_lstr_wkb_geometry,
                c.wkb_geometry AS lrs_aux_lstr_wkb_geometry

              FROM :ETL_WORK_SCHEMA.lrs_milepoint_linestring_from_to_measure_assignments AS a

                INNER JOIN :ETL_WORK_SCHEMA.lrs_milepoint_linestrings AS b
                  USING (lrs_mpt_ogc_fid, route_id, lrs_mpt_lstr_id)

                LEFT OUTER JOIN :ETL_WORK_SCHEMA.lrs_aux_linestrings AS c
                  USING (lrs_mpt_ogc_fid, route_id)
          ) AS x
      ) AS y
;

CREATE VIEW :ETL_WORK_SCHEMA.qa_lrs_milepoint_linestrings_mi_assignments_props_only
  AS
    SELECT
        lsr_mpt_aux_lstr_pair_id,
        lrs_mpt_ogc_fid,
        route_id,
        lrs_mpt_lstr_id,
        lrs_mpt_lstr_idx,
        lrs_mpt_lstr_n,
        lrs_mpt_lstr_assigned_from_measure,
        lrs_mpt_lstr_assigned_to_measure,
        lrs_milepoint_lstr_exclusive_assigned_measure_range,
        lrs_aux_table_name,
        lrs_aux_ogc_fid,
        lrs_aux_lstr_id,
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
        lrs_aux_geom_measure_exclusive_range,
        lrs_aux_geom_mi_exclusive_range,
        lrs_aux_lstr_mi_exclusive_range,
        lrs_aux_lstr_num_points,
        lrs_mpt_and_aux_lstrs_are_cospatial,
        mi_ranges_overlap,
        mi_ranges_intersection,
        mi_ranges_overlap_ft,
        passes
      FROM :ETL_WORK_SCHEMA.qa_lrs_milepoint_linestrings_mi_assignments
;

CREATE VIEW :ETL_WORK_SCHEMA.qa_lrs_milepoint_linestrings_mi_assignments_failing_mpt
  AS
    SELECT DISTINCT ON (lrs_mpt_ogc_fid)
        lrs_mpt_ogc_fid,
        route_id,
        lrs_mpt_lstr_id,
        lrs_mpt_lstr_idx,
        lrs_mpt_lstr_n,
        lrs_mpt_lstr_assigned_from_measure,
        lrs_mpt_lstr_assigned_to_measure,
        lrs_mpt_and_aux_lstrs_are_cospatial,
        mi_ranges_overlap,
        mi_ranges_overlap_ft,
        passes,
        lrs_mpt_lstr_wkb_geometry
      FROM :ETL_WORK_SCHEMA.qa_lrs_milepoint_linestrings_mi_assignments
      WHERE (
        ( NOT passes )
        AND
        ( lrs_aux_lstr_num_points > 2 )
      )
      ORDER BY lrs_mpt_ogc_fid
;

CREATE VIEW :ETL_WORK_SCHEMA.qa_lrs_milepoint_linestrings_mi_assignments_failing_aux
  AS
    SELECT DISTINCT
        lsr_mpt_aux_lstr_pair_id,
        lrs_mpt_ogc_fid,
        route_id,
        lrs_mpt_lstr_id,
        lrs_mpt_lstr_idx,
        lrs_mpt_lstr_n,
        lrs_mpt_lstr_assigned_from_measure,
        lrs_mpt_lstr_assigned_to_measure,
        lrs_milepoint_lstr_exclusive_assigned_measure_range,
        lrs_aux_table_name,
        lrs_aux_ogc_fid,
        lrs_aux_lstr_id,
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
        lrs_aux_geom_measure_exclusive_range,
        lrs_aux_geom_mi_exclusive_range,
        lrs_aux_lstr_mi_exclusive_range,
        lrs_aux_lstr_num_points,
        lrs_mpt_and_aux_lstrs_are_cospatial,
        mi_ranges_overlap,
        mi_ranges_overlap_ft,
        passes,
        lrs_aux_lstr_wkb_geometry
      FROM :ETL_WORK_SCHEMA.qa_lrs_milepoint_linestrings_mi_assignments
      WHERE (
        ( NOT passes )
        AND
        ( lrs_aux_lstr_num_points > 2 )
      )
;
