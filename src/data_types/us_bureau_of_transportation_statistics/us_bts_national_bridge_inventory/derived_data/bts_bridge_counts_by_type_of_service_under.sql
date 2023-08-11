-- https://www.fhwa.dot.gov/bridge/mtguide.pdf#page=35

COPY (
SELECT
    namelsad AS county, 
    description AS type_of_service_under_bridge,
    a.service_un AS type_of_service_under_bridge_code,
    COUNT(1) AS total_bridges
  FROM us_bureau_of_transportation_statistics.ny_bridge_inventory AS a
    INNER JOIN us_bureau_of_transportation_statistics.type_of_service_under_bridge_codes AS b
      ON ( a.service_un = b.code )
    INNER JOIN us_census_tiger.county AS c
      ON ( ( a.state_code || a.county_cod ) = c.geoid )
  GROUP BY 1,2,3
  ORDER BY 1,3
) TO STDOUT WITH CSV HEADER
;
