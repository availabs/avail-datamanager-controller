DO
  LANGUAGE plpgsql
  $$
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
