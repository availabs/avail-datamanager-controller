/*
  UNNEST the input_dataset_ids so that there exists a row for each (LCGU id, Input ID) pair

  Each LCGU polygon is associated with many input polygons where the input polygons overlap.

  NOTE: input_dataset_ids may not be unique in the LCGU table.

    Consider

      Where

          ---------------------
        A |                   |
          ---------------------

                ---------
        B       |       |
                ---------

          ---------------------
        C |                   |
          ---------------------

      LGCUs:

        ---------------------
        | A,C | A,B,C | A,C |
        ---------------------
*/

BEGIN ;

DROP TABLE IF EXISTS :OUTPUT_SCHEMA.lcgu_unnested_input_ids CASCADE ;
CREATE TABLE :OUTPUT_SCHEMA.lcgu_unnested_input_ids (
  ogc_fid           INTEGER REFERENCES :OUTPUT_SCHEMA.lcgu_output (ogc_fid),
  lcgu_input_id     INTEGER NOT NULL REFERENCES :UNION_TABLE_SCHEMA.:UNION_TABLE_NAME(ogc_fid),

  PRIMARY KEY (lcgu_input_id, ogc_fid)
) WITH (fillfactor=100) ;

INSERT INTO :OUTPUT_SCHEMA.lcgu_unnested_input_ids (
  ogc_fid,
  lcgu_input_id
)
  SELECT
      ogc_fid,
      UNNEST(input_dataset_ids) AS lgcu_input_id
    FROM :OUTPUT_SCHEMA.lcgu_output
;

CLUSTER :OUTPUT_SCHEMA.lcgu_unnested_input_ids
  USING lcgu_unnested_input_ids_pkey
;

COMMIT ;
