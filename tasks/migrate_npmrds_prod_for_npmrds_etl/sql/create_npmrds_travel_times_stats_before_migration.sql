CREATE TABLE IF NOT EXISTS _data_manager_admin.npmrds_travel_times_stats_before_migration
 AS
  SELECT
      state,
      COUNT(1) AS row_count
    FROM public.npmrds
    GROUP BY state
    ORDER BY state
;
