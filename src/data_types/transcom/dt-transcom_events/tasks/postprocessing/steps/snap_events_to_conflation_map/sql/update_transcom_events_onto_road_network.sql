DO
  LANGUAGE plpgsql
  $$
    DECLARE
      -- These variables are relevant for the PROCEDURE versioning.
      procedure_version TEXT := 'v0_0_2' ;
      conflation_map_version TEXT := 'v0_6_0' ;

      table_name TEXT ;

      event_year SMALLINT ;

      ddl TEXT ;
      ddl_arr TEXT[] ;

    BEGIN
      table_name := 'transcom_events_onto_conflation_map_' || procedure_version ;

      FOR event_year IN
        EXECUTE FORMAT('SELECT DISTINCT year FROM _transcom_admin.%I ORDER BY year ', table_name)
        LOOP

          SELECT FORMAT('
            SELECT
                a.event_id,
                a.year,

                b.event_type,
                e.general_category AS nysdot_general_category,
                e.sub_category AS nysdot_sub_category,
                e.detailed_category AS nysdot_detailed_category,

                GREATEST(
                  b.start_date_time,
                  %L::TIMESTAMP                           -- <event_year>-01-01
                ) AS start_date_time,

                LEAST(
                  b.close_date,
                  %L::TIMESTAMP - ''1 second''::INTERVAL  -- <event_year + 1>-01-01
                ) AS close_date,

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
                a.both_directions,
                c.ris,
                c.tmc,

                a._modified_timestamp AS transcom_event_modified_timestamp,

                b.point_geom    AS transcom_event_point_geom,
                a.snap_pt_geom  AS transcom_event_snapped_geom,
                c.wkb_geometry  AS conflation_map_way_geom,
                d.wkb_geometry  AS conflation_map_node_geom

              FROM _transcom_admin.%I AS a
                INNER JOIN _transcom_admin.transcom_events_expanded_view AS b
                  USING (event_id)
                INNER JOIN conflation.%I AS c
                  ON ( a.conflation_way_id = c.id )
                INNER JOIN conflation.%I AS d
                  ON ( a.conflation_node_id = d.id )
                LEFT OUTER JOIN transcom.nysdot_transcom_event_classifications AS e
                  ON ( LOWER(b.event_type) = LOWER(e.event_type) )
                  
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
