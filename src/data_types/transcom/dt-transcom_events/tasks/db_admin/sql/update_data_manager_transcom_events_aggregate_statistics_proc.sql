CREATE SCHEMA IF NOT EXISTS _transcom_admin ;

DROP PROCEDURE IF EXISTS _transcom_admin.update_data_manager_transcom_events_aggregate_statistics();

CREATE OR REPLACE PROCEDURE _transcom_admin.update_data_manager_transcom_events_aggregate_statistics()
  LANGUAGE sql
  AS $$
    UPDATE data_manager.views AS a
      SET
        start_date    = b.min_start_date_time,
        end_date      = b.max_start_date_time,
        last_updated  = b.max_modified_timestamp,
        statistics    = b.summary_statistics
      FROM (
        SELECT
            MIN(min_start_date_time) AS min_start_date_time,
            MAX(max_start_date_time) AS max_start_date_time,
            MAX(max_modified_timestamp) AS max_modified_timestamp,
            json_object_agg(
              nysdot_category,
              ct
            ) AS summary_statistics
          FROM (
            SELECT
                ( COALESCE(nysdot_general_category, '')
                  || ':'
                  || COALESCE(nysdot_sub_category, '' )
                ) AS nysdot_category,
                MIN(start_date_time) AS min_start_date_time,
                MAX(start_date_time) AS max_start_date_time,
                MAX(_modified_timestamp) AS max_modified_timestamp,
                COUNT(1) AS ct
              FROM transcom.transcom_events_aggregate
              GROUP BY 1
          ) AS c
      ) AS b
      WHERE (
        a.data_table = 'transcom.transcom_events_aggregate'
      )
    ;
  $$
;
