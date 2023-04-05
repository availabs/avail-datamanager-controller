CREATE SCHEMA IF NOT EXISTS _transcom_admin ;

DROP PROCEDURE IF EXISTS _transcom_admin.update_transcom_events_by_tmc_summary();

CREATE OR REPLACE PROCEDURE _transcom_admin.update_transcom_events_by_tmc_summary()
  LANGUAGE plpgsql
  AS $$
    DECLARE
      -- These variables are relevant for the PROCEDURE versioning.
      procedure_version TEXT := 'v0_0_1' ;

    BEGIN
      PERFORM
          set_config(
            'search_path',
            ( SELECT boot_val FROM pg_settings WHERE name='search_path' ),
            true
          )
      ;

      EXECUTE FORMAT('
          CREATE OR REPLACE VIEW _transcom_admin.%I
            AS
              SELECT
                  tmc,
                  year,
                  COALESCE(t1.accident_counts_by_type, ''{}''::JSONB) AS accident_counts_by_type,
                  COALESCE(t1.total_accidents, 0)::INTEGER AS total_accidents,
                  COALESCE(t2.construction_days_by_type, ''{}''::JSONB) AS construction_days_by_type,
                  COALESCE(t3.total_construction_days, 0)::INTEGER AS total_construction_days
                FROM (
                  SELECT DISTINCT
                      tmc,
                      year
                    FROM _transcom_admin.%I
                ) AS t0
                LEFT OUTER JOIN (
                    SELECT
                        tmc,
                        year,
                        jsonb_object_agg(
                          x.event_type,
                          x.event_type_ct
                        ) AS accident_counts_by_type,

                        SUM(x.event_type_ct) AS total_accidents

                      FROM (
                        SELECT
                            tmc,
                            year,
                            event_type,
                            count(1) AS event_type_ct
                        FROM _transcom_admin.%I AS a
                        WHERE ( event_class = ''accident'' )
                        GROUP BY tmc, year, event_type
                      ) AS x

                      GROUP BY tmc, year
                  ) AS t1 USING (tmc, year)
                  LEFT OUTER JOIN (
                    SELECT
                        tmc,
                        year,
                        jsonb_object_agg(
                          y.event_type,
                          y.total_days
                        ) AS construction_days_by_type
                      FROM (
                        SELECT
                            tmc,
                            year,
                            event_type,
                            COUNT(DISTINCT event_date) AS total_days
                          FROM (
                            SELECT
                                tmc,
                                year,
                                event_type,
                                generate_series(
                                  date_trunc(''day'', event_close_time),
                                  date_trunc(''day'', event_open_time),
                                  ''1 day''::interval
                                ) AS event_date
                              FROM _transcom_admin.%I AS a
                              WHERE ( event_class = ''construction'' )
                          ) AS x
                          GROUP BY tmc, year, event_type
                      ) AS y
                      GROUP BY tmc, year
                  ) AS t2 USING (tmc, year)
                  LEFT OUTER JOIN (
                    SELECT
                        tmc,
                        year,
                        COUNT(DISTINCT event_date) AS total_construction_days
                      FROM (
                        SELECT
                            tmc,
                            year,
                            generate_series(
                              date_trunc(''day'', event_close_time),
                              date_trunc(''day'', event_open_time),
                              ''1 day''::interval
                            ) AS event_date
                          FROM _transcom_admin.%I AS a
                          WHERE ( event_class = ''construction'' )
                      ) AS x
                      GROUP BY tmc, year
                  ) AS t3 USING (tmc, year)
          ;

        ',
        'transcom_events_by_tmc_summary_' || procedure_version,             -- CREATE
        'transcom_events_onto_road_network_' || procedure_version,          -- t0
        'transcom_events_onto_road_network_' || procedure_version,          -- t1
        'transcom_events_onto_road_network_' || procedure_version,          -- t2
        'transcom_events_onto_road_network_' || procedure_version           -- t3
      ) ;

    END;
$$;

