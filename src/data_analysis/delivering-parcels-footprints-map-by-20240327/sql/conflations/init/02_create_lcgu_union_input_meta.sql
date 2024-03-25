/*
  Link the lcgu_input_id (elements in the LCGU's input_dataset_ids array)
    back to the (dama_view_id, input_int_id) of the LCGI's input Union dataset.
*/
BEGIN ;

DROP TABLE IF EXISTS :OUTPUT_SCHEMA.lcgu_union_input_meta CASCADE ;
CREATE TABLE :OUTPUT_SCHEMA.lcgu_union_input_meta (
  ogc_fid           INTEGER REFERENCES :OUTPUT_SCHEMA.lcgu_output (ogc_fid),
  lcgu_input_id     INTEGER NOT NULL REFERENCES :UNION_TABLE_SCHEMA.:UNION_TABLE_NAME(ogc_fid),

  dama_view_id      INTEGER NOT NULL REFERENCES data_manager.views (view_id),
  union_input_id    INTEGER NOT NULL,

  FOREIGN KEY (lcgu_input_id, ogc_fid) REFERENCES :OUTPUT_SCHEMA.lcgu_unnested_input_ids(lcgu_input_id, ogc_fid),
  PRIMARY KEY (lcgu_input_id, ogc_fid)
) WITH (fillfactor=100) ;

INSERT INTO :OUTPUT_SCHEMA.lcgu_union_input_meta (
  ogc_fid,
  lcgu_input_id,
  dama_view_id,
  union_input_id
)
  SELECT
      a.ogc_fid,
      a.lcgu_input_id,

      b.dama_view_id,
      b.input_int_id AS union_input_id

    FROM :OUTPUT_SCHEMA.lcgu_unnested_input_ids AS a
      INNER JOIN :UNION_TABLE_SCHEMA.:UNION_TABLE_NAME AS b
        ON ( a.lcgu_input_id = b.ogc_fid )
;

CLUSTER :OUTPUT_SCHEMA.lcgu_union_input_meta
  USING lcgu_union_input_meta_pkey
;

COMMIT ;
