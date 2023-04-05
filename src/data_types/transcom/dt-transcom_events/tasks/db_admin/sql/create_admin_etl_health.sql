CREATE SCHEMA IF NOT EXISTS _transcom_admin;

CREATE OR REPLACE VIEW _transcom_admin.etl_health
  AS
    SELECT
        a.id AS data_manager_view_id,
        '24 hours'::INTERVAL AS expected_update_interval,
        b.actual_update_interval,
        CASE
          WHEN (actual_update_interval < '1 day'::INTERVAL)
            THEN 'GOOD'
          WHEN (actual_update_interval < '26 hours'::INTERVAL)
            THEN 'MODERATE'
          ELSE 'POOR'
        END AS etl_health_status
      FROM (
          SELECT
              id
            FROM data_manager.views
            WHERE data_table = 'transcom.transcom_events_aggregate'
        ) AS a CROSS JOIN (
          SELECT
              (NOW() - MAX(end_timestamp)) AS actual_update_interval
            FROM _transcom_admin.etl_control
        ) AS b
;
