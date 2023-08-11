COPY (
  SELECT
      classification,
      ROUND(SUM(section_length)::NUMERIC, 1) AS total_miles,
      ROUND(SUM(section_length * COALESCE(total_lanes, 2))::NUMERIC, 1) AS total_lane_miles
    FROM nysdot_roadway_inventory_system.roadway_inventory_system AS a
      INNER JOIN nysdot_roadway_inventory_system.government_agency_ownership_classifications AS b
        USING (owning_jurisdiction)
    GROUP BY 1
    ORDER BY 3 DESC
) TO STDOUT WITH CSV HEADER
;

