BEGIN ;

DROP TABLE IF EXISTS :OUTPUT_SCHEMA.lcgu_bldg_poly_value CASCADE ;
CREATE TABLE :OUTPUT_SCHEMA.lcgu_bldg_poly_value (
  ogc_fid                     INTEGER PRIMARY KEY REFERENCES :OUTPUT_SCHEMA.lcgu_output (ogc_fid),
  p_nonland_av                DOUBLE PRECISION
) WITH (fillfactor=100) ;

INSERT INTO :OUTPUT_SCHEMA.lcgu_bldg_poly_value (
  ogc_fid,
  p_nonland_av
)
  SELECT
      a.ogc_fid,
      SUM(
        ( GREATEST(b.p_total_av - b.p_land_av, 0::DOUBLE PRECISION) ) -- don't want negative numbers
        * ( a.lcgu_area_sq_meters / b.total_lcgu_bldg_sq_meters )     -- nonland_av allocation per input parcel
      ) AS p_nonland_av
    FROM (
        SELECT DISTINCT
            ogc_fid,
            p_ogc_fid,
            lcgu_area_sq_meters
          FROM :OUTPUT_SCHEMA.lcgu_poly_ancestor_properties
          WHERE ( n_ogc_fid IS NOT NULL )
      ) AS a
      INNER JOIN :OUTPUT_SCHEMA.total_building_footprint_per_parcel AS b
        USING ( p_ogc_fid )
    GROUP BY ogc_fid
;

CREATE VIEW :OUTPUT_SCHEMA.lcgu_bldg_poly_value_with_geoms
  AS
    SELECT
        ogc_fid,
        a.p_nonland_av,
        b.wkb_geometry
      FROM :OUTPUT_SCHEMA.lcgu_bldg_poly_value AS a
        INNER JOIN :OUTPUT_SCHEMA.lcgu_output AS b
          USING ( ogc_fid )
;

CLUSTER :OUTPUT_SCHEMA.lcgu_bldg_poly_value
  USING lcgu_bldg_poly_value_pkey
;

COMMIT ;
