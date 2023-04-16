CREATE SCHEMA IF NOT EXISTS _transcom_admin ;

DROP PROCEDURE IF EXISTS _transcom_admin.update_transcom_events_onto_road_network();

CREATE OR REPLACE PROCEDURE _transcom_admin.update_transcom_events_onto_road_network()
  LANGUAGE plpgsql
  AS $$
    DECLARE
      -- These variables are relevant for the PROCEDURE versioning.
      procedure_version TEXT := 'v0_0_2' ;
      conflation_map_version TEXT := 'v0_6_0' ;

      table_name TEXT ;

      event_year SMALLINT ;

      ddl TEXT ;
      ddl_arr TEXT[] ;

    BEGIN
      PERFORM
          set_config(
            'search_path',
            ( SELECT boot_val FROM pg_settings WHERE name='search_path' ),
            true
          )
      ;

      table_name := 'transcom_events_onto_conflation_map_' || procedure_version ;

      FOR event_year IN
        EXECUTE FORMAT('SELECT DISTINCT year FROM _transcom_admin.%I ORDER BY year ', table_name)
        LOOP

          SELECT FORMAT('
            SELECT
                a.event_id,
                a.year,

                b.event_type,
                b.event_class,

                GREATEST(
                  b.open_time,
                  %L::TIMESTAMP
                ) AS event_open_time,

                LEAST(
                  b.close_time,
                  %L::TIMESTAMP - ''1 second''::INTERVAL
                ) AS event_close_time,

                a.conflation_way_id,
                a.conflation_node_id,

                CASE
                  WHEN a.osm_fwd = 0 THEN -a.conflation_node_id
                  ELSE a.conflation_node_id
                END AS signed_conflation_node_id,

                c.dir,
                a.n,
                c.osm,
                a.osm_fwd,
                c.ris,
                c.tmc,

                a._modified_timestamp AS transcom_event_modified_timestamp,

                b.point_geom    AS transcom_event_point_geom,
                a.snap_pt_geom  AS transcom_event_snapped_geom,
                c.wkb_geometry  AS conflation_map_way_geom,
                d.wkb_geometry  AS conflation_map_node_geom

              FROM _transcom_admin.%I AS a
                INNER JOIN transcom.transcom_historical_events AS b
                  USING (event_id)
                INNER JOIN conflation.%I AS c
                  ON ( a.conflation_way_id = c.id )
                INNER JOIN conflation.%I AS d
                  ON ( a.conflation_node_id = d.id )
              WHERE ( a.year = %L )
          ',
          event_year::TEXT || '-01-01',
          (event_year + 1)::TEXT || '-01-01',
          table_name,
          'conflation_map_' || event_year || '_' || conflation_map_version,
          'conflation_map_' || event_year || '_nodes_' || conflation_map_version,
          event_year
        ) INTO ddl ;

        ddl_arr := ddl || ddl_arr ;

      END LOOP ;

      EXECUTE FORMAT('SELECT array_to_string(%L::TEXT[], ''
          UNION ALL''
        ) ;
        ',
        ddl_arr
      ) INTO ddl ;

      SELECT FORMAT('
        CREATE OR REPLACE VIEW _transcom_admin.%I
          AS ',
        'transcom_events_onto_road_network_' || procedure_version,
        ddl
      ) || ddl
        || '
        ;' INTO ddl;

      EXECUTE ddl;

      RAISE NOTICE 'DONE:   %', clock_timestamp();

    END;
$$;
