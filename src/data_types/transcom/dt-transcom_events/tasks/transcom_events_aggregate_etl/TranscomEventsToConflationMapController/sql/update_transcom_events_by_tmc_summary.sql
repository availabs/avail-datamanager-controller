DO
  LANGUAGE plpgsql
  $$
    DECLARE
      -- These variables are relevant for the PROCEDURE versioning.
      procedure_version TEXT := 'v0_0_2' ;

    BEGIN

      EXECUTE FORMAT('
          CREATE OR REPLACE VIEW _transcom_admin.%I
            AS
              SELECT
                  tmc,
                  year,
                  COALESCE(t1.accident_counts_by_type, ''{}''::JSONB) AS accident_counts_by_type,
                  COALESCE(t1.total_accidents, 0)::INTEGER AS total_accidents,
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
                          x.nysdot_detailed_category,
                          x.event_type_ct
                        ) AS accident_counts_by_type,

                        SUM(x.event_type_ct) AS total_accidents

                      FROM (
                        SELECT
                            tmc,
                            year,
                            nysdot_detailed_category,
                            count(1) AS event_type_ct
                        FROM _transcom_admin.%I AS a
                        WHERE ( nysdot_sub_category = ''Crash'' )
                        GROUP BY tmc, year, nysdot_detailed_category
                      ) AS x

                      GROUP BY tmc, year
                  ) AS t1 USING (tmc, year)
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
                              date_trunc(''day'', close_date),
                              date_trunc(''day'', start_date_time),
                              ''1 day''::interval
                            ) AS event_date
                          FROM _transcom_admin.%I AS a
                          WHERE ( nysdot_sub_category = ''Construction'' )
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

