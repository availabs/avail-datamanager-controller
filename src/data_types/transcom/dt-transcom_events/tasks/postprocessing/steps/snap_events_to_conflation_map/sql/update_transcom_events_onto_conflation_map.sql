DO
  LANGUAGE plpgsql
  $$
    DECLARE
      -- These variables are relevant for the PROCEDURE versioning.
      procedure_version TEXT := 'v0_0_2' ;
      conflation_map_version TEXT := 'v0_6_0' ;

      -- NOTE: a conflation_map must exist for every year in range.
      min_event_year SMALLINT := 2016 ;
      max_event_year SMALLINT := 2023 ;

      cmd TEXT ;

      table_name TEXT ;

      current_max_modified_timestamp TIMESTAMP ;

      mapping_start_timestamp TIMESTAMP := NOW() ;

      event_year SMALLINT ;

      knn_K SMALLINT := 5 ;

    BEGIN
      RAISE NOTICE 'START:   %', clock_timestamp();

      --  This table into which the TRANSCOM Events -> OSM Ways mappings are loaded
      --    is created in the _transcom_admin schema.
      --  This table will inherit from transcom.transcom_events_to_osm_ways.
      table_name := 'transcom_events_onto_conflation_map_' || procedure_version ;

      EXECUTE FORMAT ('
          CREATE TABLE IF NOT EXISTS _transcom_admin.%I (
            event_id                      TEXT,
            year                          SMALLINT,
            conflation_way_id             BIGINT NOT NULL,
            conflation_node_id            BIGINT,
            osm_fwd                       SMALLINT,
            both_directions               SMALLINT,
            n                             SMALLINT,
            _modified_timestamp           TIMESTAMP NOT NULL,

            snap_pt_geom                  public.geometry(Point, 4326) NOT NULL,

            PRIMARY KEY (event_id, year)
          ) WITH (fillfactor=100, autovacuum_enabled=false);
        ',
        table_name
      ) ;

      -- If the table has already been loaded, get the last _modified_timestamp from the table.
      EXECUTE FORMAT ('
          SELECT
              MAX(_modified_timestamp)
            FROM (
              SELECT
                  _modified_timestamp
                FROM _transcom_admin.%I
              UNION
              SELECT
                  ''1900-01-01''::TIMESTAMP AS _modified_timestamp
            ) AS t
        ',
        table_name
      ) INTO current_max_modified_timestamp ;

      RAISE NOTICE 'create tmp_transcom_events start: %', clock_timestamp();
      RAISE NOTICE '  start: %', clock_timestamp();

      EXECUTE FORMAT ('
          CREATE TEMPORARY TABLE tmp_transcom_events
            WITH (fillfactor = 100)
            ON COMMIT DROP
            AS
              SELECT
                  a.event_id,
                  b.year,
                  LOWER(a.direction) AS direction,

                  a.point_geom,

                  a._modified_timestamp

                FROM _transcom_admin.transcom_events_expanded_view AS a
                  LEFT JOIN LATERAL generate_series(
                    EXTRACT(
                      YEAR FROM start_date_time
                    )::INTEGER,
                    EXTRACT(
                      YEAR FROM COALESCE(end_all_lanes_open_to_traffic, close_date)
                    )::INTEGER
                  ) AS b(year) ON TRUE
                WHERE (
                  ( a.state = ''NY'')
                  AND
                  ( a.point_long IS NOT NULL )
                  AND
                  ( a.point_lat IS NOT NULL )
                  AND
                  ( a._modified_timestamp >= %L::TIMESTAMP )
                  AND
                  ( b.year BETWEEN %L and %L )
                )
          ;

          CREATE INDEX tmp_transcom_events_gix
            ON tmp_transcom_events
              USING GIST (point_geom)
          ;

          CLUSTER tmp_transcom_events
            USING tmp_transcom_events_gix ;

          ANALYZE tmp_transcom_events ;
        ',
        current_max_modified_timestamp,
        min_event_year,
        max_event_year
      ) ;

      RAISE NOTICE '  done:  %', clock_timestamp();

      CREATE TEMPORARY TABLE tmp_event_to_cways_knn (
        event_id            TEXT,
        c_way_id            BIGINT,
        snap_dist           DOUBLE PRECISION,
        snap_pt_geom        public.geometry(Geometry,4326),
        PRIMARY KEY (event_id, c_way_id)
      ) WITH (fillfactor=100)
        ON COMMIT DROP
      ;

      CREATE INDEX tmp_event_to_cways_knn_idx
        ON tmp_event_to_cways_knn (c_way_id)
      ;

      FOR event_year IN ( SELECT DISTINCT year FROM tmp_transcom_events ORDER BY year )
        LOOP
          RAISE NOTICE 'Event Year: %', event_year;
          RAISE NOTICE '  start: %', clock_timestamp();

          -- Clear the TEMP table to prepare for loading this event_year's events.
          TRUNCATE tmp_event_to_cways_knn ;

          EXECUTE FORMAT('
              INSERT INTO tmp_event_to_cways_knn (
                event_id,
                c_way_id,
                snap_dist,
                snap_pt_geom
              )
                SELECT
                    a.event_id,
                    b.c_way_id,
                    b.snap_dist,
                    b.snap_pt_geom
                  FROM tmp_transcom_events AS a
                    INNER JOIN LATERAL (
                      SELECT
                        id AS c_way_id,
                        ST_Distance(
                          GEOGRAPHY(a.point_geom),
                          GEOGRAPHY(x.wkb_geometry)
                        ) AS snap_dist,
                        ST_ClosestPoint(x.wkb_geometry, a.point_geom) AS snap_pt_geom
                      FROM conflation.%I AS x
                      WHERE ( x.n < 7 )
                      ORDER BY ( a.point_geom <-> x.wkb_geometry ) ASC
                      LIMIT %L -- The K of the KNN
                    ) AS b ON TRUE
                  WHERE (
                    ( a.year = %L )
                    AND
                    ( snap_dist < 1000 )
                  )
              ;

              CLUSTER tmp_event_to_cways_knn
                USING tmp_event_to_cways_knn_idx
              ;

              ANALYZE tmp_event_to_cways_knn ;
            ',
            'conflation_map_' || event_year || '_' || conflation_map_version,   -- AS x
            knn_K,                                                              -- LIMIT %L
            event_year                                                          -- WHERE clause
          ) ;

          RAISE NOTICE '  knn:   %', clock_timestamp();

          EXECUTE FORMAT('
            INSERT INTO _transcom_admin.%I (
              event_id,
              year,
              conflation_way_id,
              osm_fwd,
              both_directions,
              n,
              _modified_timestamp,
              snap_pt_geom
            )
              SELECT
                  event_id,
                  %L AS year,
                  conflation_way_id,
                  osm_fwd,
                  both_directions,
                  n,
                  _modified_timestamp,
                  snap_pt_geom
                FROM (
                  SELECT
                      event_id,
                      conflation_way_id,
                      osm_fwd,
                      both_directions,
                      n,
                      _modified_timestamp,
                      snap_pt_geom,

                      row_number() OVER (
                        PARTITION BY event_id
                        ORDER BY
                          (
                            snap_dist
                            *
                            CASE
                              WHEN is_same_dir THEN 0.5
                              ELSE 1
                            END
                          ) ASC,
                          osm_fwd ASC,
                          conflation_way_id ASC
                      ) AS rownum

                    FROM (
                      SELECT
                          a.event_id,
                          a._modified_timestamp,
                          c.id AS conflation_way_id,
                          c.osm_fwd,
                          ( a.direction = ''both directions'' )::INTEGER AS both_directions,
                          c.n,
                          b.snap_pt_geom,
                          b.snap_dist,
                          (
                            COALESCE(d.direction, ''NONE'') =
                              CASE
                                WHEN a.direction = ''northbound''  THEN ''N''
                                WHEN a.direction = ''southbound''  THEN ''S''
                                WHEN a.direction = ''eastbound''   THEN ''E''
                                WHEN a.direction = ''westbound''   THEN ''W''
                                ELSE ''NONE''
                              END
                          ) AS is_same_dir
                        FROM tmp_transcom_events AS a
                          INNER JOIN tmp_event_to_cways_knn  AS b
                            USING (event_id)
                          INNER JOIN conflation.%I AS c
                            ON ( b.c_way_id = c.id )
                          LEFT OUTER JOIN ny.%I AS d
                            USING (tmc)
                        WHERE ( a.year = %L )
                    ) AS x
                  ) AS y
                  WHERE ( rownum = 1 )

              ON CONFLICT (event_id, year) DO
                UPDATE SET
                  conflation_way_id     = EXCLUDED.conflation_way_id,
                  osm_fwd               = EXCLUDED.osm_fwd,
                  n                     = EXCLUDED.n,
                  _modified_timestamp   = EXCLUDED._modified_timestamp,
                  snap_pt_geom          = EXCLUDED.snap_pt_geom,
                  conflation_node_id    = NULL -- Important for UPDATE below.
              ;
            ',
            table_name,                                                         -- INSERT INTO
            event_year,                                                         -- AS year
            'conflation_map_' || event_year || '_' || conflation_map_version,   -- AS c
            'tmc_metadata_' || event_year,                                      -- AS d
            event_year                                                          -- x's WHERE clause
          ) ;

          RAISE NOTICE '  match: %', clock_timestamp();


          EXECUTE FORMAT ('
              UPDATE
                  _transcom_admin.%I AS t1
                SET
                  conflation_node_id = t2.conflation_node_id
                FROM (

                  SELECT
                      a.event_id,
                      a.year,
                      b.c_node_id AS conflation_node_id

                    FROM _transcom_admin.%I AS a
                      --  Here we get for each conflation_map way
                      --    the closest conflation_map node on the matched way
                      --    to the snapped event point on the way.
                      --  EG:
                      --       a---b-x--c
                      --
                      --       Where: a,b,c are confltion_map nodes
                      --              x is the event location snapped to the conflation_map way,
                      --              b is the closest conflation map node to the snapped point.
                      INNER JOIN LATERAL (
                        SELECT
                            y.c_node_id
                        FROM conflation.%I AS x                     -- conflation_map
                          INNER JOIN LATERAL (                      -- node ids for each conflation map way
                            SELECT
                                UNNEST(t.node_ids)  AS c_node_id    
                              FROM conflation.%I AS t
                              WHERE ( x.id = t.id )
                          ) AS y ON TRUE
                            INNER JOIN conflation.%I AS z           -- conflation map node geoms
                              ON ( y.c_node_id = z.id )
                        WHERE (
                          ( a.conflation_way_id = x.id )
                          AND
                          ( a.osm_fwd = x.osm_fwd )
                        )
                        ORDER BY ( a.snap_pt_geom <-> z.wkb_geometry ) ASC
                        LIMIT 1
                      ) AS b ON TRUE
                    WHERE ( a.conflation_node_id IS NULL )  -- NOTE: The insert above NULLs out this column

                ) AS t2
                WHERE (
                  ( t1.event_id = t2.event_id )
                  AND
                  ( t1.year = t2.year )
                )
            ',
            table_name,                                                               -- AS t1
            table_name,                                                               -- AS a
            'conflation_map_' || event_year || '_' || conflation_map_version,         -- AS x
            'conflation_map_' || event_year || '_ways_' || conflation_map_version,    -- AS t
            'conflation_map_' || event_year || '_nodes_' || conflation_map_version    -- AS z
          ) ;

          RAISE NOTICE '  nodes: %', clock_timestamp();

        END LOOP ;

      RAISE NOTICE 'Cluster';
      RAISE NOTICE '  start: %', clock_timestamp();

      EXECUTE FORMAT ('
          CLUSTER _transcom_admin.%I
            USING %I ;

          ANALYZE _transcom_admin.%I ;
        ',
        table_name,
        table_name || '_pkey',
        table_name
      ) ;

      DROP TABLE tmp_transcom_events ;

      RAISE NOTICE '  done:  %', clock_timestamp();

    END;
  $$;
