-- https://gis.ny.gov/system/files/documents/2022/08/nys-tax-parcels-data-dictionary.pdf

BEGIN ;

DROP TABLE IF EXISTS :OUTPUT_SCHEMA.lcgu_poly_ancestor_properties CASCADE ;
CREATE TABLE :OUTPUT_SCHEMA.lcgu_poly_ancestor_properties (
  ogc_fid               INTEGER REFERENCES :OUTPUT_SCHEMA.lcgu_output (ogc_fid),

  lcgu_area_sq_meters   DOUBLE PRECISION NOT NULL,

  p_dama_view_id        INTEGER REFERENCES data_manager.views(view_id),
  p_ogc_fid             INTEGER REFERENCES :PARCELS_TABLE_SCHEMA.:PARCELS_TABLE_NAME (ogc_fid),
  p_land_av             DOUBLE PRECISION,
  p_total_av            DOUBLE PRECISION,
  p_full_market_val     DOUBLE PRECISION,
  p_sqft_living         DOUBLE PRECISION,
  p_gross_floor_area    INTEGER,

  n_dama_view_id        INTEGER REFERENCES data_manager.views(view_id),
  n_ogc_fid             INTEGER REFERENCES :NYS_ITS_FOOTPRINTS_TABLE_SCHEMA.:NYS_ITS_FOOTPRINTS_TABLE_NAME (ogc_fid),

  UNIQUE (ogc_fid, p_ogc_fid, n_ogc_fid)
) WITH (fillfactor=100) ;

INSERT INTO :OUTPUT_SCHEMA.lcgu_poly_ancestor_properties (
  ogc_fid,
  lcgu_area_sq_meters,

  p_dama_view_id,
  p_ogc_fid,
  p_land_av,
  p_total_av,
  p_full_market_val,
  p_sqft_living,
  p_gross_floor_area,

  n_dama_view_id,
  n_ogc_fid
)
  SELECT
      a.ogc_fid,
      b.area_26918 AS lcgu_area_sq_meters,

      a.p_dama_view_id,
      a.p_ogc_fid,

      p.land_av           AS  p_land_av,
      p.total_av          AS  p_total_av,
      p.full_market_val   AS  p_full_market_val,
      p.sqft_living       AS  p_sqft_living,
      p.gfa               AS  p_gross_floor_area,

      a.n_dama_view_id,
      a.n_ogc_fid

    FROM :OUTPUT_SCHEMA.lcgu_poly_lineage AS a
      INNER JOIN :OUTPUT_SCHEMA.lcgu_output AS b
        USING (ogc_fid)
      LEFT OUTER JOIN :PARCELS_TABLE_SCHEMA.:PARCELS_TABLE_NAME AS p
        ON ( a.p_ogc_fid = p.ogc_fid )
;

CLUSTER :OUTPUT_SCHEMA.lcgu_poly_ancestor_properties
  USING lcgu_poly_ancestor_properties_ogc_fid_p_ogc_fid_n_ogc_fid_key
;

COMMIT ;
