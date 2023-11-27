DROP TABLE IF EXISTS :ETL_WORK_SCHEMA.qa_lrs_aux_features_with_inconsistent_measures ;

CREATE TABLE :ETL_WORK_SCHEMA.qa_lrs_aux_features_with_inconsistent_measures (
    lrs_aux_table_name          TEXT NOT NULL,
    lrs_aux_ogc_fid             TEXT NOT NULL,
    route_id                    TEXT NOT NULL,

    lrs_aux_geom_from_measure   DOUBLE PRECISION NOT NULL,
    lrs_aux_geom_to_measure     DOUBLE PRECISION NOT NULL,

    lrs_aux_geom_len_mi         DOUBLE PRECISION NOT NULL,

    measure_len_diff_feet       DOUBLE PRECISION NOT NULL,

    PRIMARY KEY (lrs_aux_table_name, lrs_aux_ogc_fid)
  ) WITH (fillfactor=100)
;

INSERT INTO :ETL_WORK_SCHEMA.qa_lrs_aux_features_with_inconsistent_measures (
  lrs_aux_table_name,
  lrs_aux_ogc_fid,
  route_id,

  lrs_aux_geom_from_measure,
  lrs_aux_geom_to_measure,

  lrs_aux_geom_len_mi,

  measure_len_diff_feet
)
  SELECT
      lrs_aux_table_name,
      lrs_aux_ogc_fid,
      route_id,

      lrs_aux_geom_from_measure,
      lrs_aux_geom_to_measure,

      lrs_aux_geom_len_mi,


      ROUND(
        (
          (
            ABS(lrs_aux_geom_to_measure - lrs_aux_geom_from_measure)
            - lrs_aux_geom_len_mi
          ) * 5280
        )::NUMERIC,
        2
      ) AS measure_len_diff_feet

    FROM :ETL_WORK_SCHEMA.lrs_aux_geometries
    WHERE ( NOT has_reliable_geom_from_to_measures )
;


