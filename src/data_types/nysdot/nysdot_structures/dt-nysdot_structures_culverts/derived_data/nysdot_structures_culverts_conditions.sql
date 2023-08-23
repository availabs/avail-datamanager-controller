-- NOTE: This query predates all the data integrity work done on the bridges dataset.
--       Similar work would need to be done for the culverts. 
COPY (
  SELECT
      a.bin,
      a.county,
      b.classification AS primary_owner_class,
      a.primary_own AS primary_owner,
      a.primary_mai AS primary_maintenance,
      a.condition_r AS condition_rating
    FROM nysdot_structures.culverts AS a
      INNER JOIN nysdot_structures.government_agency_ownership_classifications AS b
        ON ( a.primary_own = b.name )
    ORDER BY county, primary_owner_class, bin
) TO STDOUT WITH CSV HEADER ;
