BEGIN ;

DROP VIEW IF EXISTS :OUTPUT_SCHEMA.lcgu_bldg_poly_value_with_geoms CASCADE ;
CREATE VIEW :OUTPUT_SCHEMA.lcgu_bldg_poly_value_with_geoms
  AS
    SELECT
        a.*,
        b.wkb_geometry
      FROM :OUTPUT_SCHEMA.lcgu_bldg_poly_value AS a
        INNER JOIN :OUTPUT_SCHEMA.lcgu_output AS b
          USING ( ogc_fid )
;

COMMIT ;
