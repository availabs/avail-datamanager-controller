BEGIN ;

DROP TABLE IF EXISTS us_census_tiger.tracts_2010_2020_analysis ;

CREATE TABLE us_census_tiger.tracts_2010_2020_analysis
  AS
    SELECT
        ogc_fid_2010,
        ogc_fid_2020,
        geoid_2010,
        geoid_2020,
        
        (
          ST_Area(intersection_geom)
          / area_sq_meters_2010
        ) AS intersection_ratio_2010,

        area_sq_meters_2010,

        (
          ST_Area(intersection_geom)
          / area_sq_meters_2020
        ) AS intersection_ratio_2020,

        area_sq_meters_2020,

        diff_2010_minus_2020_geom,
        diff_2020_minus_2010_geom,

        intersection_geom

      FROM (
        SELECT
            a.ogc_fid AS ogc_fid_2010,
            b.ogc_fid AS ogc_fid_2020,

            a.geoid_10 AS geoid_2010,
            b.geoid    AS geoid_2020,

            ST_Area(GEOGRAPHY(a.wkb_geometry)) AS area_sq_meters_2010,
            ST_Area(GEOGRAPHY(b.wkb_geometry)) AS area_sq_meters_2020,

            ST_CollectionExtract(
              ST_Multi(
                ST_Difference(
                  a.wkb_geometry,
                  COALESCE(
                    b.wkb_geometry,
                    ST_SetSRID('MULTIPOLYGON EMPTY'::geometry, 4326)
                  )
                )
              ),
              3
            )::public.geometry(MultiPolygon,4326) AS diff_2010_minus_2020_geom,

            ST_CollectionExtract(
              ST_Multi(
                ST_Difference(
                  b.wkb_geometry,
                  COALESCE(
                    a.wkb_geometry,
                    ST_SetSRID('MULTIPOLYGON EMPTY'::geometry, 4326)
                  )
                )
              ),
              3
            )::public.geometry(MultiPolygon,4326) AS diff_2020_minus_2010_geom,

            COALESCE(
              ST_CollectionExtract(
                ST_Multi(
                  ST_Intersection(
                    a.wkb_geometry,
                    b.wkb_geometry
                  )
                ),
                3
              ),
              ST_SetSRID('MULTIPOLYGON EMPTY'::geometry, 4326)
            )::public.geometry(MultiPolygon,4326) AS intersection_geom

          FROM us_census_tiger.tract_2010 AS a
            FULL OUTER JOIN us_census_tiger.tract AS b
              ON ( a.geoid_10 = b.geoid )
    ) AS t
;

COMMIT ;
