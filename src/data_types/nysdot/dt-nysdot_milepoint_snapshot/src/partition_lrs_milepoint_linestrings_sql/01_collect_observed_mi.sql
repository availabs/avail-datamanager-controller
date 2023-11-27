-- FIXME FIXME FIXME: Put higher value on mpt & aux linestring start/end points when assigning mpt lstring from/to.
--                    If start/end points are equal, then threshold MUST be greater than 1. Also a <=> CHECK.

DROP TABLE IF EXISTS :ETL_WORK_SCHEMA.lrs_milepoint_linestrings_observed_milepoints ;

CREATE TABLE :ETL_WORK_SCHEMA.lrs_milepoint_linestrings_observed_milepoints (
    lrs_mpt_ogc_fid             INTEGER NOT NULL,
    lrs_mpt_lstr_id             INTEGER NOT NULL,
    lrs_mpt_lstr_idx            INTEGER NOT NULL,
    lrs_mpt_lstr_n              INTEGER NOT NULL,

    observed_mi                 DOUBLE PRECISION NOT NULL,
    observed_mi_idx             INTEGER,

    PRIMARY KEY (lrs_mpt_lstr_id, observed_mi)
  ) WITH (fillfactor=100)
;

INSERT INTO :ETL_WORK_SCHEMA.lrs_milepoint_linestrings_observed_milepoints (
  lrs_mpt_ogc_fid,
  lrs_mpt_lstr_id,
  lrs_mpt_lstr_idx,
  lrs_mpt_lstr_n,
  observed_mi
)
  SELECT DISTINCT
      a.lrs_mpt_ogc_fid,
      a.lrs_mpt_lstr_id,
      a.lrs_mpt_lstr_idx,
      a.lrs_mpt_lstr_n,

      UNNEST(
        ARRAY[
          ROUND(
            GREATEST(
              a.assigned_from_measure,
              b.lrs_aux_geom_from_mi
            )::NUMERIC,
            3
          ),

          ROUND(
            LEAST(
              a.assigned_to_measure,
              b.lrs_aux_geom_to_mi
            )::NUMERIC,
            3
          )
        ]
      ) AS observed_mi

    FROM :ETL_WORK_SCHEMA.lrs_milepoint_linestring_from_to_measure_assignments AS a
      INNER JOIN :ETL_WORK_SCHEMA.lrs_aux_linestrings AS b
        USING (route_id)

    WHERE (

      numrange(
        COALESCE(a.assigned_from_measure::NUMERIC, -1),
        COALESCE(a.assigned_to_measure::NUMERIC, -1),
        '[)'
      )
      &&
      numrange(
        LEAST(
          b.lrs_aux_geom_from_mi,
          b.lrs_aux_geom_to_mi
        )::NUMERIC,
        GREATEST(
          b.lrs_aux_geom_from_mi,
          b.lrs_aux_geom_to_mi
        )::NUMERIC,
        '[)'
      )

    )
;

INSERT INTO :ETL_WORK_SCHEMA.lrs_milepoint_linestrings_observed_milepoints (
  lrs_mpt_ogc_fid,
  lrs_mpt_lstr_id,
  lrs_mpt_lstr_idx,
  lrs_mpt_lstr_n,
  observed_mi
)
  SELECT
      a.lrs_mpt_ogc_fid,
      a.lrs_mpt_lstr_id,
      a.lrs_mpt_lstr_idx,
      a.lrs_mpt_lstr_n,
      MAX(a.assigned_to_measure) AS observed_mi

    FROM :ETL_WORK_SCHEMA.lrs_milepoint_linestring_from_to_measure_assignments AS a
      INNER JOIN :ETL_WORK_SCHEMA.lrs_milepoint_linestrings_observed_milepoints AS b
        USING (lrs_mpt_lstr_id)
    GROUP BY 1,2,3,4
    HAVING
    (
      (
        (
          MAX(a.assigned_to_measure)
          - MAX(b.observed_mi)
        ) * 5280
      ) > 10 -- the assigned measure is at least 10 feet past the max observed measure
    )
;

INSERT INTO :ETL_WORK_SCHEMA.lrs_milepoint_linestrings_observed_milepoints (
  lrs_mpt_ogc_fid,
  lrs_mpt_lstr_id,
  lrs_mpt_lstr_idx,
  lrs_mpt_lstr_n,
  observed_mi
)
  SELECT DISTINCT
      a.lrs_mpt_ogc_fid,
      a.lrs_mpt_lstr_id,
      a.lrs_mpt_lstr_idx,
      a.lrs_mpt_lstr_n,
      0 AS observed_mi

    FROM :ETL_WORK_SCHEMA.lrs_milepoint_linestrings_observed_milepoints AS a
    WHERE ( lrs_mpt_lstr_idx = 1 )
    ON CONFLICT DO NOTHING
;


INSERT INTO :ETL_WORK_SCHEMA.lrs_milepoint_linestrings_observed_milepoints (
  lrs_mpt_ogc_fid,
  lrs_mpt_lstr_id,
  lrs_mpt_lstr_idx,
  lrs_mpt_lstr_n,
  observed_mi
)
  SELECT
    a.lrs_mpt_ogc_fid,
    a.lrs_mpt_lstr_id,
    a.lrs_mpt_lstr_idx,
    a.lrs_mpt_lstr_n,

    MAX(b.observed_mi) AS observed_mi

  FROM :ETL_WORK_SCHEMA.lrs_milepoint_linestrings_observed_milepoints AS a
    INNER JOIN :ETL_WORK_SCHEMA.lrs_milepoint_linestrings_observed_milepoints AS b
      USING (lrs_mpt_ogc_fid)

  WHERE ( a.lrs_mpt_lstr_idx = (b.lrs_mpt_lstr_idx + 1) )

  GROUP BY 1,2,3,4

  ON CONFLICT DO NOTHING
;

UPDATE :ETL_WORK_SCHEMA.lrs_milepoint_linestrings_observed_milepoints AS a
  SET observed_mi_idx = b.observed_mi_idx
  FROM (
    SELECT
        lrs_mpt_lstr_id,
        observed_mi,
        ROW_NUMBER() OVER (PARTITION BY lrs_mpt_lstr_id ORDER BY observed_mi) AS observed_mi_idx
      FROM :ETL_WORK_SCHEMA.lrs_milepoint_linestrings_observed_milepoints
  ) AS b
  WHERE (
    ( a.lrs_mpt_lstr_id = b.lrs_mpt_lstr_id )
    AND
    ( a.observed_mi = b.observed_mi )
  )
;
