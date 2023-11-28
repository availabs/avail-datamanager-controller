/*

dama_dev_1=# select building_id from floodplains.enhanced_building_risk_geom except select building_id from floodplains.buildings_to_census_tracts order by 1;
 building_id 
-------------
      563851
      563854
      563855
     1221948
     1230017
(5 rows)

*/

BEGIN ;

DROP TABLE IF EXISTS floodplains.buildings_to_census_tracts ;

CREATE TABLE floodplains.buildings_to_census_tracts (
  building_id           BIGINT,
  census_tract_fips     TEXT,

  PRIMARY KEY (building_id)
) ;

INSERT INTO floodplains.buildings_to_census_tracts
  SELECT DISTINCT ON (building_id)
      a.building_id,
      b.geoid as census_tract_fips
    FROM floodplains.enhanced_building_risk_geom AS a
      INNER JOIN us_census_tiger.tract_with_divided_geoms AS b
        ON ( ST_Intersects(a.geom, b.wkb_geometry) )
    GROUP BY 1,2
    ORDER BY
        building_id,
        SUM(ST_Area(ST_Intersection(a.geom, b.wkb_geometry))) DESC
;

INSERT INTO floodplains.buildings_to_census_tracts (building_id, census_tract_fips)
  VALUES
    (563851, 36101963000),
    (563854, 36101963000),
    (563855, 36101963000),
    (1221948, 36003951201),
    (1230017, 36003951202)
;

COMMIT ;
