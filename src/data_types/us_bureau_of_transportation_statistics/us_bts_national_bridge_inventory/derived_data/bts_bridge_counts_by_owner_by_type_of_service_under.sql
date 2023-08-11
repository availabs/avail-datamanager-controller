-- https://www.fhwa.dot.gov/bridge/mtguide.pdf#page=35

COPY (
SELECT
    c.namelsad AS county, 
    b.classification AS owner_class,
    COUNT(1) AS total_bridges,
    COUNT(1) FILTER ( WHERE a.service_un IN ('5', '6', '7', '8', '9') ) AS total_bridges_over_water
  FROM us_bureau_of_transportation_statistics.national_bridge_inventory AS a

    INNER JOIN us_bureau_of_transportation_statistics.government_agency_ownership_classifications AS b
      ON ( a.owner_022 = b.code )

    INNER JOIN us_census_tiger.county AS c
      ON ( ( a.state_code || a.county_cod ) = c.geoid )

  WHERE ( a.state_code = '36' )

  GROUP BY 1,2
  ORDER BY 1,3 DESC
) TO STDOUT WITH CSV HEADER
;
