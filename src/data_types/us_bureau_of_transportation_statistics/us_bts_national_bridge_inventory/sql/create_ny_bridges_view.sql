CREATE VIEW IF NOT EXISTS ny_bridge_inventory
  AS
    SELECT
        *
      FROM us_bureau_of_transportation_statistics.national_bridge_inventory
      WHERE state_code = '36'
;
