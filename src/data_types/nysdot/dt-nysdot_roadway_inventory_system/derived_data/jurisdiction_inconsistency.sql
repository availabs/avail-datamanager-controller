--  COPY (
SELECT
    jurisdiction,
    owning_jurisdiction,
    COUNT(1)
  FROM nysdot_roadway_inventory_system.roadway_inventory_system
  WHERE ( jurisdiction <> owning_jurisdiction )
  GROUP BY 1,2
  ORDER BY 1,2
--  ) TO STDOUT WITH CSV HEADER
;
