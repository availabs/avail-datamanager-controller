-- https://blog.cleverelephant.ca/2019/11/subdivide.html

BEGIN ;

DROP TABLE IF EXISTS us_census_tiger.tract_with_divided_geoms ;

CREATE TABLE IF NOT EXISTS us_census_tiger.tract_with_divided_geoms (
  geoid         TEXT,
  wkb_geometry  public.geometry(Polygon,4326)
) ;

INSERT INTO us_census_tiger.tract_with_divided_geoms (
  geoid,
  wkb_geometry
)
  SELECT
      geoid,
      ST_Subdivide(wkb_geometry) AS wkb_geometry
    FROM us_census_tiger.tract
;

CREATE INDEX tract_with_divided_geoms_gidx
  ON us_census_tiger.tract_with_divided_geoms
  USING GIST (wkb_geometry)
;

CLUSTER us_census_tiger.tract_with_divided_geoms
  USING tract_with_divided_geoms_gidx
;

COMMIT ;
