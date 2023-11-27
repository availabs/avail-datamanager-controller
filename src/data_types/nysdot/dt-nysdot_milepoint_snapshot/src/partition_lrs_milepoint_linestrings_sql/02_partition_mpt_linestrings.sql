DROP TABLE IF EXISTS :ETL_WORK_SCHEMA.paritioned_lrs_milepoint_linestrings CASCADE ;

CREATE TABLE :ETL_WORK_SCHEMA.paritioned_lrs_milepoint_linestrings (
    lrs_mpt_lstr_slice_id   SERIAL PRIMARY KEY,
    lrs_mpt_ogc_fid         INTEGER NOT NULL,
    lrs_mpt_lstr_id         INTEGER NOT NULL,
    lrs_mpt_lstr_idx        INTEGER NOT NULL,
    lrs_mpt_lstr_n          INTEGER NOT NULL,

    lrs_mpt_lstr_slice_idx  INTEGER NOT NULL,
    lrs_mpt_lstr_slice_n    INTEGER NOT NULL,

    route_id                TEXT NOT NULL,

    from_mi                 DOUBLE PRECISION NOT NULL,
    to_mi                   DOUBLE PRECISION NOT NULL,

    wkb_geometry            public.geometry(Linestring, 4326) NOT NULL,

    UNIQUE (lrs_mpt_lstr_id, lrs_mpt_lstr_slice_idx)
  ) WITH (fillfactor=100)
;

INSERT INTO :ETL_WORK_SCHEMA.paritioned_lrs_milepoint_linestrings (
  lrs_mpt_ogc_fid,
  lrs_mpt_lstr_id,
  lrs_mpt_lstr_idx,
  lrs_mpt_lstr_n,

  lrs_mpt_lstr_slice_idx,
  lrs_mpt_lstr_slice_n,

  route_id,

  from_mi,
  to_mi,

  wkb_geometry
)
    SELECT
        lrs_mpt_ogc_fid,
        lrs_mpt_lstr_id,
        lrs_mpt_lstr_idx,
        lrs_mpt_lstr_n,

        a.observed_mi_idx AS lrs_mpt_lstr_slice_idx,
        c.lrs_mpt_lstr_slice_n,

        d.route_id,

        a.observed_mi AS from_mi,
        b.observed_mi AS to_mi,

        ST_LineSubstring(
          d.wkb_geometry,
          (a.observed_mi / c.lrs_mpt_lstr_to_mi),
          (b.observed_mi / c.lrs_mpt_lstr_to_mi)
        ) AS wkb_geometry

      FROM :ETL_WORK_SCHEMA.lrs_milepoint_linestrings_observed_milepoints AS a
        INNER JOIN :ETL_WORK_SCHEMA.lrs_milepoint_linestrings_observed_milepoints AS b
          USING (lrs_mpt_ogc_fid, lrs_mpt_lstr_id, lrs_mpt_lstr_idx, lrs_mpt_lstr_n)
        INNER JOIN (
          SELECT
              lrs_mpt_lstr_id,
              MIN(observed_mi) AS lrs_mpt_lstr_from_mi,
              MAX(observed_mi) AS lrs_mpt_lstr_to_mi,

              ( MAX(observed_mi_idx) - 1 ) AS lrs_mpt_lstr_slice_n
            FROM :ETL_WORK_SCHEMA.lrs_milepoint_linestrings_observed_milepoints
            GROUP BY 1
        ) AS c
          USING (lrs_mpt_lstr_id)
        INNER JOIN :ETL_WORK_SCHEMA.lrs_milepoint_linestrings AS d
          USING (lrs_mpt_ogc_fid, lrs_mpt_lstr_id, lrs_mpt_lstr_idx, lrs_mpt_lstr_n)
      WHERE ( ( a.observed_mi_idx + 1 ) = b.observed_mi_idx )
      ORDER BY lrs_mpt_ogc_fid, lrs_mpt_lstr_idx, from_mi
;
