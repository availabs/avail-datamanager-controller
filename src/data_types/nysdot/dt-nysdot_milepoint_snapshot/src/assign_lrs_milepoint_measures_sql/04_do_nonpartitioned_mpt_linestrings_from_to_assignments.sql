/*
    Applying Matching Models bases on Invariants.

    HARD REQUIREMENT:
      LRS Aux features join to their corresponding LRS Milepoint linestrings using route_id and from/to.

    TEST: the lrs_aux_geom_from_mi and lrs_aux_geom_to_mi

--  An ideal set of INVARIANTs for an easy decision.
--    1. Find all the set X of all (lrs_mpt_ogc_fid, lrs_aux_table_name, lrs_aux_ogc_fid) relations where
--        (a) the milepoint feature's lrs_mpt_lstr_n = 1
--        (b) the aux feature's lrs_aux_lstr_n = 1
--        (c) the geom start and end points are equal (accomodating reversed to/from aux features)
--    2. For each relation in X,
--        if all aux from/to measures agree,
--        then use the concensus from/to.
--  These feature's linestrings do not need to be split.
*/

UPDATE :ETL_WORK_SCHEMA.lrs_milepoint_linestring_from_to_measure_assignments AS x
  SET assigned_from_measure       = y.lrs_mpt_lstr_from_mi,
      assigned_to_measure         = y.lrs_mpt_lstr_to_mi,
      measure_assignment_method   = 'lrs_milepoint_feature_has_no_aux_features'
  FROM (
    SELECT DISTINCT
        a.lrs_mpt_lstr_id,
        a.lrs_mpt_lstr_from_mi,
        a.lrs_mpt_lstr_to_mi
      FROM :ETL_WORK_SCHEMA.lrs_milepoint_linestrings AS a
        LEFT OUTER JOIN :ETL_WORK_SCHEMA.lrs_aux_geometries AS b
          USING (route_id)
      WHERE ( b.route_id IS NULL )
  ) AS y

  WHERE (
    ( x.assigned_from_measure IS NULL )
    AND
    ( x.assigned_to_measure IS NULL )
    AND
    ( x.lrs_mpt_lstr_id = y.lrs_mpt_lstr_id )
  )
;

-- Using TEMP TABLE so we can enforce a constraint of one assignment per LRS Milepoint Linestring
CREATE TEMPORARY TABLE tmp_method_05_assignments (
  lrs_mpt_lstr_id           INTEGER PRIMARY KEY,
  assigned_from_measure     DOUBLE PRECISION NOT NULL,
  assigned_to_measure       DOUBLE PRECISION NOT NULL
) ;

INSERT INTO tmp_method_05_assignments (
  lrs_mpt_lstr_id,
  assigned_from_measure,
  assigned_to_measure
)
  SELECT DISTINCT
      a.lrs_mpt_lstr_id,
      MIN(a.lrs_mpt_lstr_from_mi) AS assigned_from_measure,
      MIN(a.lrs_mpt_lstr_to_mi) AS assigned_to_measure

    FROM :ETL_WORK_SCHEMA.lrs_mpt_lstr_join_aux_lstr_using_route_id AS a
      LEFT OUTER JOIN :ETL_WORK_SCHEMA.lrs_mpt_lstr_join_aux_lstr_using_route_id AS b
        --  There does not exist for the Milepoint feature lrs_mpt_ogc_fid
        --    a case where
        --      the LRS Mpt Linestring's calculated from/to mi
        --        ( calculated nder the assumption of MultiLinestring ordering captures spatial ordering )
        --      overlaps a LRS Aux Linestring's calculated
        --
        --
        --    an b.lrs_mpt_lstr for which
        --    a lrs_aux_lstr is cospatial yet
        -- that better matches another lrs_mpt_lstr
        --  If this ON condition evaluates to TRUE,
        --    it suggests that there is a better assignment of LRS Milepoint Linestring from/to mi.
        --  If the Aux Linestring is not cospatial with A, but is cospatial with B,
        --    then the B's Milepoint Lstring from/to mi range MUST overlap the Aux Linestring's.
        ON (
          -- Same LRS Milepoint feature
          ( a.lrs_mpt_ogc_fid = b.lrs_mpt_ogc_fid )
          AND
          -- Different MultiLinestring constituent Linestring
          ( a.lrs_mpt_lstr_id != b.lrs_mpt_lstr_id )
          AND
          -- Same Aux Linestring
          ( a.lrs_aux_lstr_id = b.lrs_aux_lstr_id )
          AND
          (
            -- Mpt linestring A is not cospatial with the Aux linestring
            ( NOT a.lrs_mpt_and_aux_lstrs_are_cospatial )
            -- Mpt linestring A's from/to overlaps the aux linestring's from/to,
            -- even though the aux linestring from/to should overlap mpt lstr B's from/to.
            AND
            (
              a.lrs_mpt_lstr_mi_exclusive_range
              &&
              b.lrs_aux_geom_mi_exclusive_range
            )
          )
          AND
          (
            -- FIXME: If lrs_aux_lstr_n = 1, then simply use the feature property from/to.
            -- Mpt linestring B is cospatial with the Aux linestring
            ( b.lrs_mpt_and_aux_lstrs_are_cospatial )
            AND
            ( -- OK if the LRS Aux feature spans both. Just cannot span a and not b.
              NOT (
                b.lrs_mpt_lstr_mi_exclusive_range
                &&
                b.lrs_aux_geom_mi_exclusive_range
              )
            )
          )
        )
      WHERE ( b.lrs_mpt_lstr_id IS NULL ) -- The conditions of the ON clause MUST evaluate to false.

    GROUP BY 1
    HAVING (
      -- For all LRS Aux Lstrs, cospatial implies range intersection
      ( BOOL_AND(
          ( NOT  ( a.lrs_mpt_and_aux_lstrs_are_cospatial ) )
          OR
          (
            a.lrs_mpt_lstr_mi_exclusive_range
            &&
            a.lrs_aux_geom_mi_exclusive_range
          )
        )
      )
    )
;

UPDATE :ETL_WORK_SCHEMA.lrs_milepoint_linestring_from_to_measure_assignments AS x

  SET assigned_from_measure       = y.assigned_from_measure,
      assigned_to_measure         = y.assigned_to_measure,
      measure_assignment_method   = 'cospatial_mpt_and_aux_implies_mi_range_overlap'

  FROM tmp_method_05_assignments AS y

  WHERE (
    ( x.assigned_from_measure IS NULL )
    AND
    ( x.assigned_to_measure IS NULL )
    AND
    ( x.lrs_mpt_lstr_id = y.lrs_mpt_lstr_id )
  )
;

UPDATE :ETL_WORK_SCHEMA.lrs_milepoint_linestring_from_to_measure_assignments AS x

  SET assigned_from_measure       = y.lrs_mpt_lstr_from_mi,
      assigned_to_measure         = y.lrs_mpt_lstr_to_mi,
      measure_assignment_method   = 'lrs_milepoint_lstr_has_no_aux_features'

  FROM (
    SELECT DISTINCT
        a.lrs_mpt_lstr_id,
        MIN(a.lrs_mpt_lstr_from_mi) AS lrs_mpt_lstr_from_mi,
        MIN(a.lrs_mpt_lstr_to_mi)   AS lrs_mpt_lstr_to_mi
      FROM :ETL_WORK_SCHEMA.lrs_mpt_lstr_join_aux_lstr_using_route_id AS a
      GROUP BY 1
      HAVING (
        -- ¬∃ any aux linestrings where the exclusive mi range overlaps this lrs_mpt_lstr's mi range.
        NOT BOOL_OR (
          (
            a.lrs_mpt_lstr_mi_exclusive_range
            &&
            a.lrs_aux_geom_mi_exclusive_range
          )
        )
      )
  ) AS y

  WHERE (
    ( x.assigned_from_measure IS NULL )
    AND
    ( x.assigned_to_measure IS NULL )
    AND
    ( x.lrs_mpt_lstr_id = y.lrs_mpt_lstr_id )
  )
;


DROP VIEW IF EXISTS :ETL_WORK_SCHEMA.debug_method_05 CASCADE ;
CREATE VIEW :ETL_WORK_SCHEMA.debug_method_05
  AS
    SELECT
        b.*
      FROM :ETL_WORK_SCHEMA.lrs_mpt_lstr_join_aux_lstr_using_route_id AS a
        LEFT OUTER JOIN :ETL_WORK_SCHEMA.lrs_mpt_lstr_join_aux_lstr_using_route_id AS b
          --  There does not exist for the Milepoint feature lrs_mpt_ogc_fid
          --    a case where
          --      the LRS Mpt Linestring's calculated from/to mi
          --        ( calculated nder the assumption of MultiLinestring ordering captures spatial ordering )
          --      overlaps a LRS Aux Linestring's calculated
          --
          --
          --    an b.lrs_mpt_lstr for which
          --    a lrs_aux_lstr is cospatial yet
          -- that better matches another lrs_mpt_lstr
          --  If this ON condition evaluates to TRUE,
          --    it suggests that there is a better assignment of LRS Milepoint Linestring from/to mi.
          --  If the Aux Linestring is not cospatial with A, but is cospatial with B,
          --    then the B's Milepoint Lstring from/to mi range MUST overlap the Aux Linestring's.
          ON (
            -- Same LRS Milepoint feature
            ( a.lrs_mpt_ogc_fid = b.lrs_mpt_ogc_fid )
            AND
            -- Different MultiLinestring constituent Linestring
            ( a.lrs_mpt_lstr_id != b.lrs_mpt_lstr_id )
            AND
            -- Same Aux Linestring
            ( a.lrs_aux_lstr_id = b.lrs_aux_lstr_id )
            AND
            (
              -- Mpt linestring A is not cospatial with the Aux linestring
              ( NOT a.lrs_mpt_and_aux_lstrs_are_cospatial )
              -- Mpt linestring A's from/to overlaps the aux linestring's from/to,
              -- even though the aux linestring from/to should overlap mpt lstr B's from/to.
              AND
              (
                a.lrs_mpt_lstr_mi_exclusive_range
                &&
                b.lrs_aux_geom_mi_exclusive_range
              )
            )
            AND
            (
              -- FIXME: If lrs_aux_lstr_n = 1, then simply use the feature property from/to.
              -- Mpt linestring B is cospatial with the Aux linestring
              ( b.lrs_mpt_and_aux_lstrs_are_cospatial )
              AND
              ( -- OK if the LRS Aux feature spans both. Just cannot span a and not b.
                NOT (
                  b.lrs_mpt_lstr_mi_exclusive_range
                  &&
                  b.lrs_aux_geom_mi_exclusive_range
                )
              )
            )
          )
        WHERE ( b.lrs_mpt_lstr_id IS NOT NULL ) -- The conditions of the ON clause MUST evaluate to false.
;
