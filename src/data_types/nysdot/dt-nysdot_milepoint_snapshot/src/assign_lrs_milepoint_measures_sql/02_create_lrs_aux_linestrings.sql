DROP TABLE IF EXISTS :ETL_WORK_SCHEMA.lrs_aux_linestrings CASCADE ;

CREATE TABLE :ETL_WORK_SCHEMA.lrs_aux_linestrings (
  lrs_aux_lstr_id                         SERIAL PRIMARY KEY,

  lrs_aux_table_name                      TEXT NOT NULL,
  lrs_aux_ogc_fid                         INTEGER NOT NULL,

  lrs_mpt_ogc_fid                         INTEGER NOT NULL,
  route_id                                TEXT NOT NULL,

  lrs_aux_geom_from_measure               DOUBLE PRECISION NOT NULL,
  lrs_aux_geom_to_measure                 DOUBLE PRECISION NOT NULL,


  lrs_aux_geom_from_to_diff               DOUBLE PRECISION NOT NULL,
  from_lt_to_measure                      BOOLEAN NOT NULL,

  lrs_mpt_from_date                       TIMESTAMP WITH TIME ZONE NOT NULL,
  lrs_aux_from_date                       TIMESTAMP WITH TIME ZONE NOT NULL,
  consistent_from_date                    BOOLEAN NOT NULL,

  lrs_aux_lstr_idx                        INTEGER NOT NULL,
  lrs_aux_lstr_n                          INTEGER NOT NULL,

  lrs_aux_geom_shape_length               DOUBLE PRECISION NOT NULL,
  lrs_aux_geom_len_mi                     DOUBLE PRECISION NOT NULL,
  lrs_aux_lstr_len_mi                     DOUBLE PRECISION NOT NULL,

  lrs_aux_geom_from_mi                    DOUBLE PRECISION NOT NULL,
  lrs_aux_geom_to_mi                      DOUBLE PRECISION NOT NULL,


  lrs_aux_lstr_from_mi                    DOUBLE PRECISION NOT NULL,
  lrs_aux_lstr_to_mi                      DOUBLE PRECISION NOT NULL,

  lrs_aux_geom_measure_exclusive_range    NUMRANGE NOT NULL,
  lrs_aux_geom_mi_exclusive_range         NUMRANGE NOT NULL,
  lrs_aux_lstr_mi_exclusive_range         NUMRANGE NOT NULL,

  has_reliable_geom_from_to_measures      BOOLEAN NOT NULL,

  lrs_aux_geom_start_pt_wkb_geometry      public.geometry(Point, 4326),
  lrs_aux_geom_end_pt_wkb_geometry        public.geometry(Point, 4326),

  lrs_aux_lstr_start_pt_wkb_geometry      public.geometry(Point, 4326),
  lrs_aux_lstr_end_pt_wkb_geometry        public.geometry(Point, 4326),

  wkb_geometry                            public.geometry(LineString, 4326),

  UNIQUE ( lrs_aux_table_name, lrs_aux_ogc_fid, lrs_aux_lstr_idx )
) WITH (fillfactor=100) ;

CREATE VIEW :ETL_WORK_SCHEMA.lrs_aux_linestrings_props_only
  AS
    SELECT
        lrs_aux_lstr_id,

        lrs_aux_table_name,
        lrs_aux_ogc_fid,

        lrs_mpt_ogc_fid,
        route_id,

        lrs_aux_geom_from_measure,
        lrs_aux_geom_to_measure,


        lrs_aux_geom_from_to_diff,
        from_lt_to_measure,

        -- Omitting these so psql can render results horizontally in console.
        --  lrs_mpt_from_date,
        --  lrs_aux_from_date,

        consistent_from_date,

        lrs_aux_lstr_idx,
        lrs_aux_lstr_n,

        lrs_aux_geom_shape_length,
        lrs_aux_geom_len_mi,
        lrs_aux_lstr_len_mi,

        lrs_aux_geom_from_mi,
        lrs_aux_geom_to_mi,

        lrs_aux_lstr_from_mi,
        lrs_aux_lstr_to_mi,

        lrs_aux_geom_measure_exclusive_range,
        lrs_aux_geom_mi_exclusive_range,
        lrs_aux_lstr_mi_exclusive_range,

        has_reliable_geom_from_to_measures

      FROM :ETL_WORK_SCHEMA.lrs_aux_linestrings
;

INSERT INTO :ETL_WORK_SCHEMA.lrs_aux_linestrings (
  lrs_aux_table_name,
  lrs_aux_ogc_fid,
  lrs_mpt_ogc_fid,
  route_id,
  lrs_aux_geom_from_measure,
  lrs_aux_geom_to_measure,


  lrs_aux_geom_from_to_diff,
  from_lt_to_measure,

  lrs_mpt_from_date,
  lrs_aux_from_date,
  consistent_from_date,
  lrs_aux_lstr_idx,
  lrs_aux_lstr_n,

  lrs_aux_geom_shape_length,
  lrs_aux_geom_len_mi,
  lrs_aux_lstr_len_mi,

  lrs_aux_geom_from_mi,
  lrs_aux_geom_to_mi,


  lrs_aux_lstr_from_mi,
  lrs_aux_lstr_to_mi,

  lrs_aux_geom_measure_exclusive_range,
  lrs_aux_geom_mi_exclusive_range,
  lrs_aux_lstr_mi_exclusive_range,

  has_reliable_geom_from_to_measures,

  lrs_aux_geom_start_pt_wkb_geometry,
  lrs_aux_geom_end_pt_wkb_geometry,

  lrs_aux_lstr_start_pt_wkb_geometry,
  lrs_aux_lstr_end_pt_wkb_geometry,

  wkb_geometry
)
  SELECT
      lrs_aux_table_name,
      lrs_aux_ogc_fid,
      lrs_mpt_ogc_fid,
      route_id,
      lrs_aux_geom_from_measure,
      lrs_aux_geom_to_measure,


      lrs_aux_geom_from_to_diff,
      from_lt_to_measure,

      lrs_mpt_from_date,
      lrs_aux_from_date,
      consistent_from_date,
      lrs_aux_lstr_idx,
      lrs_aux_lstr_n,

      lrs_aux_geom_shape_length,
      lrs_aux_geom_len_mi,
      lrs_aux_lstr_len_mi,

      lrs_aux_geom_from_mi,
      lrs_aux_geom_to_mi,

      lrs_aux_lstr_from_mi,
      lrs_aux_lstr_to_mi,

      lrs_aux_geom_measure_exclusive_range,
      numrange(
        LEAST(lrs_aux_geom_from_mi, lrs_aux_geom_to_mi)::NUMERIC,
        GREATEST(lrs_aux_geom_from_mi, lrs_aux_geom_to_mi)::NUMERIC,
        '()'
      ) AS lrs_aux_geom_mi_exclusive_range,
      lrs_aux_lstr_mi_exclusive_range,

      has_reliable_geom_from_to_measures,

      lrs_aux_geom_start_pt_wkb_geometry,
      lrs_aux_geom_end_pt_wkb_geometry,

      lrs_aux_lstr_start_pt_wkb_geometry,
      lrs_aux_lstr_end_pt_wkb_geometry,

      wkb_geometry
    FROM (
      SELECT
          lrs_aux_table_name,
          lrs_aux_ogc_fid,
          lrs_mpt_ogc_fid,
          route_id,

          lrs_aux_geom_from_measure,
          lrs_aux_geom_to_measure,


          lrs_aux_geom_from_to_diff,
          from_lt_to_measure,

          lrs_mpt_from_date,
          lrs_aux_from_date,
          consistent_from_date,
          lrs_aux_lstr_idx,
          lrs_aux_lstr_n,

          lrs_aux_geom_shape_length,
          lrs_aux_geom_len_mi,

          lrs_aux_lstr_len_mi,

          (
            LEAST(lrs_aux_geom_from_measure, lrs_aux_geom_to_measure)
            + lrs_aux_lstr_from_mi
          ) AS lrs_aux_geom_from_mi,

          (
            LEAST(lrs_aux_geom_from_measure, lrs_aux_geom_to_measure)
            + lrs_aux_lstr_to_mi
          ) AS lrs_aux_geom_to_mi,

          lrs_aux_lstr_from_mi,
          lrs_aux_lstr_to_mi,

          lrs_aux_geom_measure_exclusive_range,
          numrange(
            LEAST(lrs_aux_lstr_from_mi, lrs_aux_lstr_to_mi)::NUMERIC,
            GREATEST(lrs_aux_lstr_from_mi, lrs_aux_lstr_to_mi)::NUMERIC,
            '()'
          ) AS lrs_aux_lstr_mi_exclusive_range,

          has_reliable_geom_from_to_measures,

          lrs_aux_geom_start_pt_wkb_geometry,
          lrs_aux_geom_end_pt_wkb_geometry,

          lrs_aux_lstr_start_pt_wkb_geometry,
          lrs_aux_lstr_end_pt_wkb_geometry,

          wkb_geometry

        FROM (
          SELECT
              lrs_aux_table_name,
              lrs_aux_ogc_fid,
              route_id,
              lrs_mpt_ogc_fid,
              lrs_aux_geom_from_measure,
              lrs_aux_geom_to_measure,


              lrs_aux_geom_from_to_diff,
              from_lt_to_measure,

              lrs_mpt_from_date,
              lrs_aux_from_date,
              consistent_from_date,
              lrs_aux_lstr_idx,

              (
                COUNT(1)
                  OVER ( PARTITION BY lrs_aux_table_name, lrs_aux_ogc_fid )
              ) AS lrs_aux_lstr_n,

              lrs_aux_geom_shape_length,
              lrs_aux_geom_len_mi,
              lrs_aux_lstr_len_mi,

              lrs_aux_geom_measure_exclusive_range,

              has_reliable_geom_from_to_measures,

              -- FIXME FIXME FIXME
              -- FIXME: Need to handle reversed cases where to < from.

              CASE
                WHEN (from_lt_to_measure)
                  THEN
                    COALESCE(
                      SUM(lrs_aux_lstr_len_mi)
                        OVER (
                          PARTITION BY lrs_aux_table_name, lrs_aux_ogc_fid
                          ORDER BY lrs_aux_lstr_idx
                          ROWS UNBOUNDED PRECEDING EXCLUDE CURRENT ROW
                        ),
                      0
                    )
                ELSE
                    COALESCE(
                      SUM(lrs_aux_lstr_len_mi)
                        OVER (
                          PARTITION BY lrs_aux_table_name, lrs_aux_ogc_fid
                          ORDER BY lrs_aux_lstr_idx DESC
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ),
                      0
                    )
              END AS lrs_aux_lstr_from_mi,

              CASE
                WHEN (from_lt_to_measure)
                  THEN
                    (
                      SUM(lrs_aux_lstr_len_mi)
                        OVER (
                          PARTITION BY lrs_aux_table_name, lrs_aux_ogc_fid
                          ORDER BY lrs_aux_lstr_idx
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                      )
                    )
                ELSE
                    COALESCE(
                      SUM(lrs_aux_lstr_len_mi)
                        OVER (
                          PARTITION BY lrs_aux_table_name, lrs_aux_ogc_fid
                          ORDER BY lrs_aux_lstr_idx DESC
                          ROWS UNBOUNDED PRECEDING EXCLUDE CURRENT ROW
                        ),
                      0
                    )
              END AS lrs_aux_lstr_to_mi,

              lrs_aux_geom_start_pt_wkb_geometry,
              lrs_aux_geom_end_pt_wkb_geometry,

              lrs_aux_lstr_start_pt_wkb_geometry,
              lrs_aux_lstr_end_pt_wkb_geometry,

              wkb_geometry
            FROM (
              SELECT
                  lrs_aux_table_name,
                  lrs_aux_ogc_fid,
                  route_id,
                  lrs_mpt_ogc_fid,
                  lrs_aux_geom_from_measure,
                  lrs_aux_geom_to_measure,

                  lrs_aux_geom_measure_exclusive_range,

                  lrs_aux_geom_from_to_diff,
                  (lrs_aux_geom_from_measure < lrs_aux_geom_to_measure) AS from_lt_to_measure,

                  lrs_mpt_from_date,
                  lrs_aux_from_date,
                  ( lrs_mpt_from_date = lrs_aux_from_date ) AS consistent_from_date,
                  (geom_dump).path[1] AS lrs_aux_lstr_idx,

                  lrs_aux_geom_shape_length,
                  lrs_aux_geom_len_mi,
                  has_reliable_geom_from_to_measures,

                  (
                    ST_Length(
                      ST_Transform(
                        (geom_dump).geom,
                        26918
                      )
                    ) * 0.00062136987761928244
                  ) AS lrs_aux_lstr_len_mi,

                  lrs_aux_geom_start_pt_wkb_geometry,
                  lrs_aux_geom_end_pt_wkb_geometry,

                  ST_StartPoint((geom_dump).geom) AS lrs_aux_lstr_start_pt_wkb_geometry,
                  ST_EndPoint((geom_dump).geom) AS lrs_aux_lstr_end_pt_wkb_geometry,

                  (geom_dump).geom AS wkb_geometry
                FROM (
                  SELECT
                      a.lrs_aux_table_name,
                      a.lrs_aux_ogc_fid,

                      b.lrs_mpt_ogc_fid,
                      a.route_id,

                      a.lrs_aux_geom_from_measure,
                      a.lrs_aux_geom_to_measure,

                      numrange(
                        LEAST(a.lrs_aux_geom_from_measure, a.lrs_aux_geom_to_measure)::NUMERIC,
                        GREATEST(a.lrs_aux_geom_from_measure, a.lrs_aux_geom_to_measure)::NUMERIC,
                        '()'
                      ) AS lrs_aux_geom_measure_exclusive_range,

                      ( a.lrs_aux_geom_from_measure - a.lrs_aux_geom_to_measure ) AS lrs_aux_geom_from_to_diff,

                      b.lrs_mpt_from_date,
                      a.lrs_aux_from_date,

                      lrs_aux_geom_shape_length,

                      a.lrs_aux_geom_len_mi,
                      a.has_reliable_geom_from_to_measures,

                      ST_StartPoint(a.wkb_geometry) AS lrs_aux_geom_start_pt_wkb_geometry,
                      ST_EndPoint(a.wkb_geometry) AS lrs_aux_geom_end_pt_wkb_geometry,

                      ST_Dump(
                        ST_Force2D(a.wkb_geometry)
                      ) AS geom_dump

                    FROM :ETL_WORK_SCHEMA.lrs_aux_geometries AS a
                      INNER JOIN :ETL_WORK_SCHEMA.lrs_milepoint_linestrings AS b
                        USING ( route_id )
                    WHERE ( b.lrs_mpt_lstr_idx = 1 ) -- prevent dupes, lrs_mpt_from_date is same for all lrs_mpt_lstr_idx
                ) AS w
            ) AS x
        ) AS y
    ) AS z
;

CREATE INDEX lrs_aux_linestrings_route_id_idx
  ON :ETL_WORK_SCHEMA.lrs_aux_linestrings (route_id)
;

CREATE INDEX lrs_aux_linestrings_geom_idx
  ON :ETL_WORK_SCHEMA.lrs_aux_linestrings
  USING GIST(wkb_geometry)
;

CLUSTER :ETL_WORK_SCHEMA.lrs_aux_linestrings
  USING lrs_aux_linestrings_geom_idx
;
