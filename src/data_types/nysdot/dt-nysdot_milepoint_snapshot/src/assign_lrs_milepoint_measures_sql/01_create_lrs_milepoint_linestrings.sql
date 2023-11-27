DROP TABLE IF EXISTS :ETL_WORK_SCHEMA.lrs_milepoint_linestrings CASCADE ;

CREATE TABLE :ETL_WORK_SCHEMA.lrs_milepoint_linestrings (
  lrs_mpt_lstr_id                       SERIAL PRIMARY KEY,

  lrs_mpt_ogc_fid                       INTEGER NOT NULL,
  route_id                              TEXT NOT NULL,

  lrs_mpt_from_date                     TIMESTAMP WITH TIME ZONE NOT NULL,

  lrs_mpt_lstr_idx                      INTEGER NOT NULL,
  lrs_mpt_lstr_n                        INTEGER NOT NULL,

  lrs_mpt_geom_shape_length             DOUBLE PRECISION NOT NULL,

  lrs_mpt_geom_len_mi                   DOUBLE PRECISION NOT NULL,
  lrs_mpt_lstr_len_mi                   DOUBLE PRECISION NOT NULL,


  lrs_mpt_lstr_from_mi                  DOUBLE PRECISION NOT NULL,
  lrs_mpt_lstr_to_mi                    DOUBLE PRECISION NOT NULL,

  lrs_mpt_geom_mi_exclusive_range       NUMRANGE NOT NULL,
  lrs_mpt_lstr_mi_exclusive_range       NUMRANGE NOT NULL,

  lrs_mpt_geom_start_pt_wkb_geometry    public.geometry(Point, 4326),
  lrs_mpt_geom_end_pt_wkb_geometry      public.geometry(Point, 4326),

  lrs_mpt_lstr_start_pt_wkb_geometry    public.geometry(Point, 4326),
  lrs_mpt_lstr_end_pt_wkb_geometry      public.geometry(Point, 4326),

  wkb_geometry                          public.geometry(LineString, 4326),

  UNIQUE ( lrs_mpt_ogc_fid, lrs_mpt_lstr_idx ),
  UNIQUE ( route_id, lrs_mpt_lstr_idx ) -- TODO: Move this to the premises boolean vector
) WITH (fillfactor=100) ;

CREATE VIEW :ETL_WORK_SCHEMA.lrs_milepoint_linestrings_props_only
  AS
    SELECT
        lrs_mpt_lstr_id,

        lrs_mpt_ogc_fid,
        route_id,

        lrs_mpt_from_date,

        lrs_mpt_lstr_idx,
        lrs_mpt_lstr_n,

        lrs_mpt_geom_shape_length,
        lrs_mpt_geom_len_mi,
        lrs_mpt_lstr_len_mi,


        lrs_mpt_lstr_from_mi,
        lrs_mpt_lstr_to_mi,

        lrs_mpt_geom_mi_exclusive_range,
        lrs_mpt_lstr_mi_exclusive_range

      FROM :ETL_WORK_SCHEMA.lrs_milepoint_linestrings
;

INSERT INTO :ETL_WORK_SCHEMA.lrs_milepoint_linestrings (
  lrs_mpt_ogc_fid,
  route_id,
  lrs_mpt_from_date,
  lrs_mpt_lstr_idx,
  lrs_mpt_lstr_n,

  lrs_mpt_geom_shape_length,
  lrs_mpt_geom_len_mi,
  lrs_mpt_lstr_len_mi,


  lrs_mpt_lstr_from_mi,
  lrs_mpt_lstr_to_mi,

  lrs_mpt_geom_mi_exclusive_range,
  lrs_mpt_lstr_mi_exclusive_range,

  lrs_mpt_geom_start_pt_wkb_geometry,
  lrs_mpt_geom_end_pt_wkb_geometry,

  lrs_mpt_lstr_start_pt_wkb_geometry,
  lrs_mpt_lstr_end_pt_wkb_geometry,

  wkb_geometry
)
  SELECT
      lrs_mpt_ogc_fid,
      route_id,
      lrs_mpt_from_date,
      lrs_mpt_lstr_idx,
      lrs_mpt_lstr_n,

      lrs_mpt_geom_shape_length,
      lrs_mpt_geom_len_mi,
      lrs_mpt_lstr_len_mi,


      lrs_mpt_lstr_from_mi,
      lrs_mpt_lstr_to_mi,

      lrs_mpt_geom_mi_exclusive_range,

      numrange(
        lrs_mpt_lstr_from_mi::NUMERIC,
        lrs_mpt_lstr_to_mi::NUMERIC,
        '()'
      ) AS lrs_mpt_lstr_mi_exclusive_range,

      lrs_mpt_geom_start_pt_wkb_geometry,
      lrs_mpt_geom_end_pt_wkb_geometry,

      lrs_mpt_lstr_start_pt_wkb_geometry,
      lrs_mpt_lstr_end_pt_wkb_geometry,

      wkb_geometry
    FROM (
      SELECT
          lrs_mpt_ogc_fid,
          route_id,
          lrs_mpt_from_date,
          lrs_mpt_lstr_idx,
          lrs_mpt_lstr_n,

          lrs_mpt_geom_shape_length,

          lrs_mpt_geom_len_mi,
          lrs_mpt_lstr_len_mi,

          lrs_mpt_geom_mi_exclusive_range,

          COALESCE(
            SUM(lrs_mpt_lstr_len_mi)
              OVER (
                PARTITION BY lrs_mpt_ogc_fid
                ORDER BY lrs_mpt_lstr_idx
                ROWS UNBOUNDED PRECEDING EXCLUDE CURRENT ROW
              ),
            0
          ) AS lrs_mpt_lstr_from_mi,

          (
            SUM(lrs_mpt_lstr_len_mi)
              OVER (
                PARTITION BY lrs_mpt_ogc_fid
                ORDER BY lrs_mpt_lstr_idx
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
          ) AS lrs_mpt_lstr_to_mi,

          lrs_mpt_geom_start_pt_wkb_geometry,
          lrs_mpt_geom_end_pt_wkb_geometry,

          lrs_mpt_lstr_start_pt_wkb_geometry,
          lrs_mpt_lstr_end_pt_wkb_geometry,

          wkb_geometry
        FROM (
          SELECT
              lrs_mpt_ogc_fid,
              route_id,
              lrs_mpt_from_date,
              (geom_dump).path[1] AS lrs_mpt_lstr_idx,
              lrs_mpt_lstr_n,

              lrs_mpt_geom_shape_length,
              lrs_mpt_geom_len_mi,

              (
                ST_Length(
                  ST_Transform(
                    (geom_dump).geom,
                    26918
                  )
                ) * 0.00062136987761928244
              ) AS lrs_mpt_lstr_len_mi,

              numrange(
                0::NUMERIC,
                lrs_mpt_geom_len_mi::NUMERIC,
                '()'
              ) AS lrs_mpt_geom_mi_exclusive_range,

              lrs_mpt_geom_start_pt_wkb_geometry,
              lrs_mpt_geom_end_pt_wkb_geometry,

              ST_StartPoint((geom_dump).geom) AS lrs_mpt_lstr_start_pt_wkb_geometry,
              ST_EndPoint((geom_dump).geom) AS lrs_mpt_lstr_end_pt_wkb_geometry,

              (geom_dump).geom AS wkb_geometry
            FROM (
              SELECT
                  ogc_fid AS lrs_mpt_ogc_fid,
                  route_id,
                  from_date AS lrs_mpt_from_date,
                  ST_NumGeometries(wkb_geometry) AS lrs_mpt_lstr_n,

                  shape_length AS lrs_mpt_geom_shape_length,

                  (
                    ST_Length(
                      ST_Transform(
                        wkb_geometry,
                        26918
                      )
                    ) * 0.00062136987761928244
                  ) AS lrs_mpt_geom_len_mi,

                  ST_StartPoint(wkb_geometry) AS lrs_mpt_geom_start_pt_wkb_geometry,
                  ST_EndPoint(wkb_geometry) AS lrs_mpt_geom_end_pt_wkb_geometry,

                  ST_Dump(
                    ST_Force2D(wkb_geometry)
                  ) AS geom_dump

                FROM :SOURCE_DATA_SCHEMA.lrsn_milepoint
                WHERE (
                  ( to_date IS NULL )
-- AND
-- ( county = 1 )
                )
            ) AS x
        ) AS y
    ) AS z
;

CREATE INDEX lrs_milepoint_linestrings_route_id_idx
  ON :ETL_WORK_SCHEMA.lrs_milepoint_linestrings (route_id)
;

CREATE INDEX lrs_milepoint_linestrings_geom_idx
  ON :ETL_WORK_SCHEMA.lrs_milepoint_linestrings
  USING GIST(wkb_geometry)
;

CLUSTER :ETL_WORK_SCHEMA.lrs_milepoint_linestrings
  USING lrs_milepoint_linestrings_geom_idx
;
