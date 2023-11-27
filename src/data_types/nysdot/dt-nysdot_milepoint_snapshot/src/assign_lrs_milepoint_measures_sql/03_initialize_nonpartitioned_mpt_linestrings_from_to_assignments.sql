--  Opportunistic.
--  Will allow eventual and iterative accomodation of source data edge/degenerate cases.
--    E.G.: Can load 2023 while still working on old data's particular integrity issues.

DROP TABLE IF EXISTS :ETL_WORK_SCHEMA.lrs_milepoint_linestring_from_to_measure_assignments CASCADE ;

CREATE TABLE :ETL_WORK_SCHEMA.lrs_milepoint_linestring_from_to_measure_assignments (
  lrs_mpt_ogc_fid                 INTEGER NOT NULL REFERENCES :SOURCE_DATA_SCHEMA.lrsn_milepoint(ogc_fid),

  route_id                        TEXT NOT NULL,

  lrs_mpt_lstr_id                 INTEGER PRIMARY KEY REFERENCES :ETL_WORK_SCHEMA.lrs_milepoint_linestrings(lrs_mpt_lstr_id),
  lrs_mpt_lstr_idx                INTEGER NOT NULL,
  lrs_mpt_lstr_n                  INTEGER NOT NULL,

  assigned_from_measure           DOUBLE PRECISION,
  assigned_to_measure             DOUBLE PRECISION,

  measure_assignment_method       TEXT
  -- NOTE: Part of this table's pattern is adding columns. See below ALTER TABLE ADD COLUMN statements.
) ;

DROP VIEW IF EXISTS :ETL_WORK_SCHEMA.lrs_milepoint_linestrings_unassigned_measure ;
CREATE VIEW :ETL_WORK_SCHEMA.lrs_milepoint_linestrings_unassigned_measure
  AS
    SELECT
        a.*
      FROM :ETL_WORK_SCHEMA.lrs_milepoint_linestrings AS a
        INNER JOIN :ETL_WORK_SCHEMA.lrs_milepoint_linestring_from_to_measure_assignments AS b
          USING ( lrs_mpt_lstr_id )
      WHERE (
        ( b.assigned_from_measure IS NULL )
        OR
        ( b.assigned_to_measure IS NULL )
      )
;

INSERT INTO :ETL_WORK_SCHEMA.lrs_milepoint_linestring_from_to_measure_assignments (
  lrs_mpt_lstr_id,

  lrs_mpt_ogc_fid,
  route_id,

  lrs_mpt_lstr_idx,
  lrs_mpt_lstr_n
)
  SELECT
      lrs_mpt_lstr_id,

      lrs_mpt_ogc_fid,
      route_id,

      lrs_mpt_lstr_idx,
      lrs_mpt_lstr_n
    FROM :ETL_WORK_SCHEMA.lrs_milepoint_linestrings
;

CLUSTER :ETL_WORK_SCHEMA.lrs_milepoint_linestring_from_to_measure_assignments
  USING lrs_milepoint_linestring_from_to_measure_assignments_pkey
;

CLUSTER :ETL_WORK_SCHEMA.lrs_milepoint_linestrings
  USING lrs_milepoint_linestrings_route_id_idx
;

CLUSTER :ETL_WORK_SCHEMA.lrs_aux_linestrings
  USING lrs_aux_linestrings_route_id_idx
;


DROP VIEW IF EXISTS :ETL_WORK_SCHEMA.lrs_mpt_lstr_join_aux_lstr_using_route_id CASCADE;
CREATE VIEW :ETL_WORK_SCHEMA.lrs_mpt_lstr_join_aux_lstr_using_route_id
  AS
    SELECT
        a.lrs_mpt_ogc_fid,
        route_id,

        a.lrs_mpt_lstr_id,
        a.lrs_mpt_lstr_idx,
        a.lrs_mpt_lstr_n,

        -- FIXME FIXME FIXME: Verify that all lrs milepoint from/to should be (0, n) with n > 0.
        0 AS lrs_mpt_geom_from_mi,
        a.lrs_mpt_geom_len_mi AS lrs_mpt_geom_to_mi,

        a.lrs_mpt_lstr_from_mi,
        a.lrs_mpt_lstr_to_mi,

        a.lrs_mpt_geom_mi_exclusive_range,
        a.lrs_mpt_lstr_mi_exclusive_range,

        b.lrs_aux_table_name,
        b.lrs_aux_ogc_fid,

        b.lrs_aux_lstr_id,
        b.lrs_aux_lstr_idx,
        b.lrs_aux_lstr_n,

        b.lrs_aux_geom_from_measure,
        b.lrs_aux_geom_to_measure,


        b.lrs_aux_geom_from_mi,
        b.lrs_aux_geom_to_mi,

        b.lrs_aux_lstr_from_mi,
        b.lrs_aux_lstr_to_mi,

        b.lrs_aux_geom_measure_exclusive_range,
        b.lrs_aux_geom_mi_exclusive_range,
        b.lrs_aux_lstr_mi_exclusive_range,

        b.from_lt_to_measure AS lrs_aux_from_lt_to_measure,

        ST_Equals(
          a.lrs_mpt_geom_start_pt_wkb_geometry,
          CASE
            WHEN (from_lt_to_measure)
              THEN b.lrs_aux_geom_start_pt_wkb_geometry
            ELSE b.lrs_aux_geom_end_pt_wkb_geometry
          END
        ) AS lrs_mpt_and_aux_geom_start_pts_equal,

        ST_Equals(
          a.lrs_mpt_geom_end_pt_wkb_geometry,
          CASE
            WHEN (from_lt_to_measure)
              THEN b.lrs_aux_geom_end_pt_wkb_geometry
            ELSE b.lrs_aux_geom_start_pt_wkb_geometry
          END
        ) AS lrs_mpt_and_aux_geom_end_pts_equal,

        (
          ( ST_Intersects(a.wkb_geometry, b.wkb_geometry) )
          AND
          ( NOT ST_Touches(a.wkb_geometry, b.wkb_geometry) )
        ) AS lrs_mpt_and_aux_lstrs_are_cospatial,

        a.wkb_geometry AS lrs_mpt_lstr_wkb_geometry,
        b.wkb_geometry AS lrs_aux_lstr_wkb_geometry

      FROM :ETL_WORK_SCHEMA.lrs_milepoint_linestrings AS a
        INNER JOIN :ETL_WORK_SCHEMA.lrs_aux_linestrings AS b
          USING (route_id)

      -- Only consider LRS Aux features with reliable from/to measures.
      WHERE ( b.has_reliable_geom_from_to_measures )
;


CREATE VIEW :ETL_WORK_SCHEMA.lrs_mpt_lstr_join_aux_lstr_using_route_id_props_only
  AS
    SELECT
        lrs_mpt_ogc_fid,
        route_id,

        lrs_mpt_lstr_id,
        lrs_mpt_lstr_idx,
        lrs_mpt_lstr_n,

        lrs_mpt_geom_from_mi,
        lrs_mpt_geom_to_mi,

        lrs_mpt_lstr_from_mi,
        lrs_mpt_lstr_to_mi,

        lrs_mpt_geom_mi_exclusive_range,
        lrs_mpt_lstr_mi_exclusive_range,

        lrs_aux_table_name,
        lrs_aux_ogc_fid,

        lrs_aux_lstr_id,
        lrs_aux_lstr_idx,
        lrs_aux_lstr_n,

        lrs_aux_geom_from_measure,
        lrs_aux_geom_to_measure,
        lrs_aux_geom_measure_exclusive_range,

        lrs_aux_geom_from_mi,
        lrs_aux_geom_to_mi,
        lrs_aux_geom_mi_exclusive_range,

        lrs_aux_lstr_from_mi,
        lrs_aux_lstr_to_mi,
        lrs_aux_lstr_mi_exclusive_range,

        lrs_aux_from_lt_to_measure,

        lrs_mpt_and_aux_geom_start_pts_equal,
        lrs_mpt_and_aux_geom_end_pts_equal,

        lrs_mpt_and_aux_lstrs_are_cospatial

      FROM :ETL_WORK_SCHEMA.lrs_mpt_lstr_join_aux_lstr_using_route_id
;



CREATE VIEW :ETL_WORK_SCHEMA.unassigned_measures_lrs_mpt_lstr_join_aux_lstr_using_route_id
  AS
    SELECT
        a.*
      FROM :ETL_WORK_SCHEMA.lrs_mpt_lstr_join_aux_lstr_using_route_id AS a
        INNER JOIN :ETL_WORK_SCHEMA.lrs_milepoint_linestring_from_to_measure_assignments AS b
          USING (lrs_mpt_lstr_id)
      WHERE (
        ( b.assigned_from_measure IS NULL )
        AND
        ( b.assigned_to_measure IS NULL )
      )
;

CREATE VIEW :ETL_WORK_SCHEMA.unassigned_measures_lrs_mpt_lstr_join_aux_lstr_using_route_id_props_only
  AS
    SELECT
        a.*
      FROM :ETL_WORK_SCHEMA.lrs_mpt_lstr_join_aux_lstr_using_route_id_props_only AS a
        INNER JOIN :ETL_WORK_SCHEMA.lrs_milepoint_linestring_from_to_measure_assignments AS b
          USING (lrs_mpt_lstr_id)
      WHERE (
        ( b.assigned_from_measure IS NULL )
        AND
        ( b.assigned_to_measure IS NULL )
      )
;
