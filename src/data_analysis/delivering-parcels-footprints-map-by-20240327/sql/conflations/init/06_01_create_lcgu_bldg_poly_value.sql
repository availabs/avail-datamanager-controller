BEGIN ;

DROP TABLE IF EXISTS :OUTPUT_SCHEMA.lcgu_bldg_poly_value CASCADE ;
CREATE TABLE :OUTPUT_SCHEMA.lcgu_bldg_poly_value (
  ogc_fid                     INTEGER PRIMARY KEY REFERENCES :OUTPUT_SCHEMA.lcgu_output (ogc_fid),
  p_nonland_av                DOUBLE PRECISION,
  p_ogc_fid_arr               INTEGER[]
) WITH (fillfactor=100) ;

INSERT INTO :OUTPUT_SCHEMA.lcgu_bldg_poly_value (
  ogc_fid,
  p_nonland_av,
  p_ogc_fid_arr
)
  SELECT
      a.ogc_fid,
      SUM(
        ( GREATEST(b.p_total_av - b.p_land_av, 0::DOUBLE PRECISION) ) -- don't want negative numbers
        * ( a.lcgu_area_sq_meters / b.total_lcgu_bldg_sq_meters )     -- nonland_av allocation per input parcel
      ) AS p_nonland_av,
      -- NOTE:  Intentionally leaving off DISTINCT in ARRAY_AGG because there MUST be no dupes coming from subquery a.
      --        This INVARIANT is later tested in QA rather than using a CONSTRAINT TRIGGER so any violations
      --        are available for inspection/debugging.
      ARRAY_AGG( p_ogc_fid ORDER BY p_ogc_fid ) AS p_ogc_fid_arr
    FROM (
        SELECT DISTINCT
            ogc_fid,
            p_ogc_fid,
            lcgu_area_sq_meters
          FROM :OUTPUT_SCHEMA.lcgu_poly_ancestor_properties
          WHERE ( n_ogc_fid IS NOT NULL )
      ) AS a
      LEFT OUTER JOIN :OUTPUT_SCHEMA.total_building_footprint_per_parcel AS b
        USING ( p_ogc_fid )
    GROUP BY ogc_fid
;

CLUSTER :OUTPUT_SCHEMA.lcgu_bldg_poly_value
  USING lcgu_bldg_poly_value_pkey
;

COMMIT ;
