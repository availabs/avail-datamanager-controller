CREATE TABLE IF NOT EXISTS _data_manager_admin.npmrds_travel_times_stats_before_migration
 AS
  SELECT
      state,
      MD5(
        string_agg(
          travel_time_all_vehicles::TEXT, '|' ORDER BY tmc, date, epoch
        )
      ) AS travel_times_md5sum,
      COUNT(1) AS row_count
    FROM public.npmrds
    GROUP BY state
    ORDER BY state
;
