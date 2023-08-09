--  COPY (
  SELECT
      county_name,
      owning_jurisdiction,
      ROUND(SUM(section_length)::NUMERIC, 1) AS total_miles,
      ROUND(SUM(section_length * COALESCE(total_lanes, 2))::NUMERIC, 1) AS total_lane_miles
    FROM nysdot_roadway_inventory_system.roadway_inventory_system
    GROUP BY 1,2
    ORDER BY 1,4 DESC
--  ) TO STDOUT WITH CSV HEADER
;

