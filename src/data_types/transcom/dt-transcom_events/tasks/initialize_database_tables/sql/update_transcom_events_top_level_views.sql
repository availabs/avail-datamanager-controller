CREATE SCHEMA IF NOT EXISTS _transcom_admin ;

DROP PROCEDURE IF EXISTS _transcom_admin.update_transcom_events_top_level_views();

CREATE OR REPLACE PROCEDURE _transcom_admin.update_transcom_events_top_level_views()
  LANGUAGE plpgsql
  AS $$
    DECLARE
      -- These variables are relevant for the PROCEDURE versioning.
      -- NOTE: If the version changed, will need to uniherit the previous version
      --       from transcom_events_onto_conflation_map. The below code DOES NOT do that.

      procedure_version TEXT := 'v0_0_1' ;

    BEGIN
      PERFORM
          set_config(
            'search_path',
            ( SELECT boot_val FROM pg_settings WHERE name='search_path' ),
            true
          )
      ;

      IF NOT EXISTS (
          SELECT
              1
            FROM pg_catalog.pg_inherits
            WHERE inhparent = 'transcom.transcom_events_onto_conflation_map'::regclass
        ) THEN
          EXECUTE FORMAT('
              ALTER TABLE _transcom_admin.%I
                INHERIT transcom.transcom_events_onto_conflation_map
            ',
            'transcom_events_onto_conflation_map_' || procedure_version
          ) ;
      END IF ;

      EXECUTE FORMAT('
          DROP MATERIALIZED VIEW IF EXISTS transcom.transcom_events_onto_road_network ;

          CREATE MATERIALIZED VIEW IF NOT EXISTS transcom.transcom_events_onto_road_network
            WITH (fillfactor=100)
            AS
              SELECT
                  *
                FROM _transcom_admin.%I
          ;

          CREATE INDEX transcom_events_onto_road_network_pkey
            ON transcom.transcom_events_onto_road_network (event_id, year)
            WITH (fillfactor=100)
          ;

          CREATE INDEX transcom_events_onto_road_network_tmc_idx
            ON transcom.transcom_events_onto_road_network (tmc, year)
            WITH (fillfactor=100)
          ;

          CLUSTER transcom.transcom_events_onto_road_network
            USING transcom_events_onto_road_network_pkey
          ;

        ',
        'transcom_events_onto_road_network_' || procedure_version
      ) ;

      EXECUTE FORMAT('
          DROP MATERIALIZED VIEW IF EXISTS transcom_events_by_tmc_summary ;

          CREATE MATERIALIZED VIEW IF NOT EXISTS transcom.transcom_events_by_tmc_summary
            WITH (fillfactor=100)
            AS
              SELECT
                  *
                FROM _transcom_admin.%I
          ;

          DROP INDEX IF EXISTS transcom.transcom_events_by_tmc_summary_pkey ;

          CREATE INDEX transcom_events_by_tmc_summary_pkey
            ON transcom.transcom_events_by_tmc_summary (tmc, year)
            WITH (fillfactor=100)
          ;

          CLUSTER transcom.transcom_events_by_tmc_summary
            USING transcom_events_by_tmc_summary_pkey
          ;
        ',
        'transcom_events_by_tmc_summary_' || procedure_version
      ) ;

      EXECUTE FORMAT('
          DROP MATERIALIZED VIEW IF EXISTS _transcom_admin.%I ;

          CREATE MATERIALIZED VIEW _transcom_admin.%I
            AS
              SELECT
                  event_id,
                  year,

                  ST_MakeLine(
                    transcom_event_point_geom,
                    transcom_event_snapped_geom
                  ) AS event_to_snapped_pt_line,

                  ST_Distance(
                    GEOGRAPHY(transcom_event_point_geom),
                    GEOGRAPHY(transcom_event_snapped_geom)
                  ) AS event_to_snapped_dist_meters,

                  ST_MakeLine(
                    transcom_event_point_geom,
                    conflation_map_node_geom
                  ) AS event_to_node_pt_line,

                  ST_Distance(
                    GEOGRAPHY(transcom_event_point_geom),
                    GEOGRAPHY(conflation_map_node_geom)
                  ) AS event_to_node_dist_meters

                FROM _transcom_admin.%I
            ;
        ',
        'qa_transcom_events_onto_road_network_' || procedure_version,
        'qa_transcom_events_onto_road_network_' || procedure_version,
        'transcom_events_onto_road_network_' || procedure_version
      ) ;


  END;
$$;
