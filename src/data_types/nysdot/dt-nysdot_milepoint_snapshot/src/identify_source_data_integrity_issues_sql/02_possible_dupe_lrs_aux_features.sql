DROP TABLE IF EXISTS :ETL_WORK_SCHEMA.qa_possible_lrs_aux_duplicate_data_analysis ;

CREATE TABLE :ETL_WORK_SCHEMA.qa_possible_lrs_aux_duplicate_data_analysis (
    lrs_aux_table_name          TEXT NOT NULL,
    dupe_lrs_aux_ogc_fids       INTEGER[] NOT NULL,

    route_id                    TEXT NOT NULL,
    from_measure                DOUBLE PRECISION NOT NULL,
    to_measure                  DOUBLE PRECISION NOT NULL,

    dupe_data                   JSONB NOT NULL,

    PRIMARY KEY (
      lrs_aux_table_name,
      dupe_lrs_aux_ogc_fids
    )
  ) WITH (fillfactor=100)
;

-- Creating these TEMPORARY VIEWs because we cannot reference :ETL_WORK_SCHEMA variable in DO BLOCK.
CREATE TEMPORARY VIEW tmp_qa_dupe_lrs_aux_features_analysis
  AS
    SELECT
        *
      FROM :ETL_WORK_SCHEMA.qa_possible_lrs_aux_duplicate_data_analysis
;

CREATE TEMPORARY TABLE tmp_source_data_schema
  AS
    SELECT :'SOURCE_DATA_SCHEMA' AS source_data_schema
;

CREATE TEMPORARY TABLE tmp_qa_possible_dupe_lrs_aux_features (
    lrs_aux_table_name          TEXT NOT NULL,
    route_id                    TEXT NOT NULL,

    lrs_aux_geom_from_measure   DOUBLE PRECISION NOT NULL,
    lrs_aux_geom_to_measure     DOUBLE PRECISION NOT NULL,

    dupe_lrs_aux_ogc_fids       INTEGER[] NOT NULL,
    dupe_count                  INTEGER NOT NULL,

    PRIMARY KEY (
      lrs_aux_table_name,
      route_id,
      lrs_aux_geom_from_measure,
      lrs_aux_geom_to_measure
    )
  ) WITH (fillfactor=100)
;

INSERT INTO tmp_qa_possible_dupe_lrs_aux_features (
  lrs_aux_table_name,
  route_id,

  lrs_aux_geom_from_measure,
  lrs_aux_geom_to_measure,

  dupe_lrs_aux_ogc_fids,
  dupe_count
)
  SELECT
      lrs_aux_table_name,
      route_id,

      lrs_aux_geom_from_measure,
      lrs_aux_geom_to_measure,

      ARRAY_AGG(lrs_aux_ogc_fid ORDER BY lrs_aux_ogc_fid) AS dupe_lrs_aux_ogc_fids,

      COUNT(1) AS dupe_count

    FROM :ETL_WORK_SCHEMA.lrs_aux_geometries

    GROUP BY
        lrs_aux_table_name,
        route_id,
        lrs_aux_geom_from_measure,
        lrs_aux_geom_to_measure

    HAVING ( COUNT(1) > 1 )
;


DO $$

  DECLARE
    table_schema  TEXT ;
    table_name    TEXT ;
    query         TEXT ;
    data          TEXT ;

  BEGIN

    SELECT
        source_data_schema
      FROM tmp_source_data_schema
      INTO table_schema
    ;

    FOR table_name IN
      SELECT DISTINCT
          lrs_aux_table_name
        FROM tmp_qa_possible_dupe_lrs_aux_features
        ORDER BY 1
    LOOP

      query := FORMAT(
        E'
          INSERT INTO tmp_qa_dupe_lrs_aux_features_analysis (
            lrs_aux_table_name,
            dupe_lrs_aux_ogc_fids,

            route_id,
            from_measure,
            to_measure,

            dupe_data
          )
            SELECT
                %L AS lrs_aux_table_name,
                ARRAY_AGG(ogc_fid ORDER BY ogc_fid) AS dupe_lrs_aux_ogc_fids,
                route_id,
                from_measure,
                to_measure,
                row_data
              FROM (
                SELECT
                    ogc_fid,
                    a.route_id,
                    a.from_measure,
                    a.to_measure,
                    (
                      row_to_json(a)::JSONB
                        - ''ogc_fid''
                        - ''event_id''
                        - ''aud_date_create''
                        - ''aud_date_update''
                        - ''aud_user_create''
                        - ''aud_user_update''
                        - ''wkb_geometry''
                    ) AS row_data
                  FROM %I.%I AS a
                    INNER JOIN tmp_qa_possible_dupe_lrs_aux_features AS b
                      ON (
                        ( a.ogc_fid = ANY(b.dupe_lrs_aux_ogc_fids) )
                        AND
                        ( b.lrs_aux_table_name = %L )
                        AND
                        ( a.route_id = b.route_id )
                        AND
                        ( a.from_measure = b.lrs_aux_geom_from_measure )
                        AND
                        ( a.to_measure = b.lrs_aux_geom_to_measure )
                      )
              ) AS t
              GROUP BY route_id, from_measure, to_measure, row_data
              HAVING ( COUNT(1) > 1 )
            ;
        ',
          -- SELECT AS lrs_aux_table_name
          table_name,
          -- FROM AS a
          table_schema,
          table_name,
          -- ON AND lrs_aux_table_name
          table_name
      ) ;

      EXECUTE query ;

    END LOOP ;

END $$ ;

