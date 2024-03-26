BEGIN ;

CREATE VIEW :OUTPUT_SCHEMA.lcgu_bldg_poly_value_with_geoms
  AS
    SELECT
        ogc_fid,
        a.p_nonland_av,
        a.p_ogc_fid_arr,
        b.wkb_geometry
      FROM :OUTPUT_SCHEMA.lcgu_bldg_poly_value AS a
        INNER JOIN :OUTPUT_SCHEMA.lcgu_output AS b
          USING ( ogc_fid )
;

COMMIT ;
