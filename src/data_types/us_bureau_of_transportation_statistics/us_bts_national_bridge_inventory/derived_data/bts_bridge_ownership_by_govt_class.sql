COPY (
SELECT
    classification AS govt_owner_class,
    COUNT(1) AS total_bridges
  FROM us_bureau_of_transportation_statistics.ny_bridge_inventory AS a
    INNER JOIN us_bureau_of_transportation_statistics.government_agency_ownership_classifications AS b
      ON (a.owner_022 = b.code )
  GROUP BY 1
  ORDER BY 2 DESC
) TO STDOUT WITH CSV HEADER
;
