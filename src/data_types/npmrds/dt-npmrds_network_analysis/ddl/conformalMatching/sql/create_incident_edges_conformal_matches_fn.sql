-- FIXME: Very slow. Would probably help to replace ST_Distance with <->.
--        Starting with a KNN approach would also enable more sophisticated matching logic.
--
-- FIXME: Performs poorly for overlapping TmcLinears and places poor cartography dual carriageways.
--
--  CREATE OR REPLACE FUNCTION __NETWORK_SPATIAL_ANALYSIS_SCHEMA_NAME__.incident_edges_conformal_matches (

DROP TABLE IF EXISTS npmrds_network_spatial_analysis.incident_edges_conformal_match_queries ;

CREATE TABLE npmrds_network_spatial_analysis.incident_edges_conformal_match_queries (
  match_level             INTEGER PRIMARY KEY,
  max_spatial_tolerance   INTEGER NOT NULL,
  match_query             TEXT NOT NULL
) ;

INSERT INTO npmrds_network_spatial_analysis.incident_edges_conformal_match_queries (
  match_level,
  max_spatial_tolerance,
  match_query
) VALUES
  (
    1,
    10,
    '
      INSERT INTO %I (
        node_id_a,
        node_id_b,
        label,
        label_fields
      )
        SELECT DISTINCT
            x.node_id_a,
            x.node_id_b,
            x.label::JSONB,
            x.label_fields
          FROM (
            SELECT
                a.node_id AS node_id_a,
                a.wkb_geometry::TEXT AS wkb_geometry_a,

                b.node_id AS node_id_b,
                b.wkb_geometry::TEXT AS wkb_geometry_b,

                jsonb_build_array(''tmc'', ''linear_id'', ''direction'') AS label_fields,

                (
                  jsonb_build_object(
                    ''INBOUND'',
                    json_agg(
                      json_build_array(tmc, linear_id, direction)
                        ORDER BY a.bearing, linear_id, direction, tmc
                    ) FILTER (
                      WHERE a.traversal_direction = ''INBOUND''
                    ),

                    ''OUTBOUND'',
                    json_agg(
                      json_build_array(tmc, linear_id, direction)
                        ORDER BY a.bearing, linear_id, direction, tmc
                    ) FILTER (
                      WHERE a.traversal_direction = ''OUTBOUND''
                    )
                  )
                ) AS label

              FROM %s AS a
                INNER JOIN %s AS b
                  USING (tmc, linear_id, direction, traversal_direction)
              WHERE (
                ( ABS( a.bearing - b.bearing ) <= ( 0.5 * %s ) /*degrees*/ )
                AND
                (
                  public.ST_Distance(
                    a.wkb_geometry::public.geography,
                    b.wkb_geometry::public.geography
                  ) <= ( 0.5 * %s ) /*meter*/
                )
              )
              GROUP BY 1, 2, 3, 4
          ) AS x
            INNER JOIN (
              SELECT
                  node_id AS node_id_a,
                  wkb_geometry::TEXT AS wkb_geometry_a,

                  (
                    jsonb_build_object(
                      ''INBOUND'',
                      json_agg(
                        json_build_array(tmc, linear_id, direction)
                          ORDER BY bearing, linear_id, direction, tmc
                      ) FILTER (
                        WHERE traversal_direction = ''INBOUND''
                      ),

                      ''OUTBOUND'',
                      json_agg(
                        json_build_array(tmc, linear_id, direction)
                          ORDER BY bearing, linear_id, direction, tmc
                      ) FILTER (
                        WHERE traversal_direction = ''OUTBOUND''
                      )
                    )
                  ) AS label
                FROM %s
                GROUP BY 1, 2
            ) AS y
              USING ( node_id_a, wkb_geometry_a, label )
            INNER JOIN (
              SELECT
                  node_id AS node_id_b,
                  wkb_geometry::TEXT AS wkb_geometry_b,

                  (
                    jsonb_build_object(
                      ''INBOUND'',
                      json_agg(
                        json_build_array(tmc, linear_id, direction)
                          ORDER BY bearing, linear_id, direction, tmc
                      ) FILTER (
                        WHERE traversal_direction = ''INBOUND''
                      ),

                      ''OUTBOUND'',
                      json_agg(
                        json_build_array(tmc, linear_id, direction)
                          ORDER BY bearing, linear_id, direction, tmc
                      ) FILTER (
                        WHERE traversal_direction = ''OUTBOUND''
                      )
                    )
                  ) AS label
                FROM %s
                GROUP BY 1, 2
            ) AS z
            USING ( node_id_b, wkb_geometry_b, label )
      ;
    '
  ),
  (
    2,
    10,
    '
      INSERT INTO %I (
        node_id_a,
        node_id_b,
        label,
        label_fields
      )
        SELECT DISTINCT
            x.node_id_a,
            x.node_id_b,
            x.label::JSONB,
            x.label_fields
          FROM (
            SELECT
                a.node_id AS node_id_a,
                a.wkb_geometry::TEXT AS wkb_geometry_a,

                b.node_id AS node_id_b,
                b.wkb_geometry::TEXT AS wkb_geometry_b,

                jsonb_build_array(''linear_id'', ''direction'') AS label_fields,

                (
                  json_build_array(
                    json_agg(
                      json_build_array(linear_id, direction)
                        ORDER BY a.bearing, linear_id, direction
                    ) FILTER (
                      WHERE traversal_direction = ''INBOUND''
                    ),

                    json_agg(
                      json_build_array(linear_id, direction)
                        ORDER BY a.bearing, linear_id, direction
                    ) FILTER (
                      WHERE traversal_direction = ''OUTBOUND''
                    )
                  )::TEXT
                ) AS label

              FROM %s AS a
                INNER JOIN %s AS b
                  USING (linear_id, direction, traversal_direction)
              WHERE (
                ( ABS( a.bearing - b.bearing ) <= ( 1 * %s ) /*degrees*/ )
                AND
                (
                  public.ST_Distance(
                    a.wkb_geometry::public.geography,
                    b.wkb_geometry::public.geography
                  ) <= ( 1 * %s ) /*meter*/
                )
              )
              GROUP BY 1, 2, 3, 4
          ) AS x
            INNER JOIN (
              SELECT
                  node_id AS node_id_a,
                  wkb_geometry::TEXT AS wkb_geometry_a,

                  (
                    json_build_array(
                      json_agg(
                        json_build_array(linear_id, direction)
                          ORDER BY bearing, linear_id, direction
                      ) FILTER (
                        WHERE traversal_direction = ''INBOUND''
                      ),

                      json_agg(
                        json_build_array(linear_id, direction)
                          ORDER BY bearing, linear_id, direction
                      ) FILTER (
                        WHERE traversal_direction = ''OUTBOUND''
                      )
                    )::TEXT
                  ) AS label
                FROM %s
                GROUP BY 1, 2
            ) AS y
              USING ( node_id_a, wkb_geometry_a, label )
            INNER JOIN (
              SELECT
                  node_id AS node_id_b,
                  wkb_geometry::TEXT AS wkb_geometry_b,

                  (
                    json_build_array(
                      json_agg(
                        json_build_array(linear_id, direction)
                          ORDER BY bearing, linear_id, direction
                      ) FILTER (
                        WHERE traversal_direction = ''INBOUND''
                      ),

                      json_agg(
                        json_build_array(linear_id, direction)
                          ORDER BY bearing, linear_id, direction
                      ) FILTER (
                        WHERE traversal_direction = ''OUTBOUND''
                      )
                    )::TEXT
                  ) AS label
                FROM %s
                GROUP BY 1, 2
            ) AS z
            USING ( node_id_b, wkb_geometry_b, label )
      ;
    '
  ),
  (
    3,
    10,
    '
      INSERT INTO %I (
        node_id_a,
        node_id_b,
        label,
        label_fields
      )
        SELECT DISTINCT
            x.node_id_a,
            x.node_id_b,
            x.label::JSONB,
            x.label_fields
          FROM (
            SELECT
                a.node_id AS node_id_a,
                a.wkb_geometry::TEXT AS wkb_geometry_a,

                b.node_id AS node_id_b,
                b.wkb_geometry::TEXT AS wkb_geometry_b,

                jsonb_build_array(''linear_id'') AS label_fields,

                (
                  json_build_array(
                    json_agg(
                      DISTINCT linear_id ORDER BY linear_id
                    ) FILTER (
                      WHERE traversal_direction = ''INBOUND''
                    ),

                    json_agg(
                      DISTINCT linear_id ORDER BY linear_id
                    ) FILTER (
                      WHERE traversal_direction = ''OUTBOUND''
                    )
                  )::TEXT
                ) AS label

              FROM %s AS a
                INNER JOIN %s AS b
                  USING ( linear_id, traversal_direction )
              WHERE (
                ( ABS( a.bearing - b.bearing ) <= ( 3 * %s ) /*degrees*/ )
                AND
                (
                  public.ST_Distance(
                    a.wkb_geometry::public.geography,
                    b.wkb_geometry::public.geography
                  ) <= ( 1.5 * %s ) /*meter*/
                )
              )
              GROUP BY 1, 2, 3, 4
              HAVING ( COUNT( DISTINCT linear_id ) > 1 )
          ) AS x
            INNER JOIN (
              SELECT
                  node_id AS node_id_a,
                  wkb_geometry::TEXT AS wkb_geometry_a,

                  (
                    json_build_array(
                      json_agg(
                        DISTINCT linear_id ORDER BY linear_id
                      ) FILTER (
                        WHERE traversal_direction = ''INBOUND''
                      ),

                      json_agg(
                        DISTINCT linear_id ORDER BY linear_id
                      ) FILTER (
                        WHERE traversal_direction = ''OUTBOUND''
                      )
                    )::TEXT
                  ) AS label
                FROM %s
                GROUP BY 1, 2
            ) AS y
              USING ( node_id_a, wkb_geometry_a, label )
            INNER JOIN (
              SELECT
                  node_id AS node_id_b,
                  wkb_geometry::TEXT AS wkb_geometry_b,

                  (
                    json_build_array(
                      json_agg(
                        DISTINCT linear_id ORDER BY linear_id
                      ) FILTER (
                        WHERE traversal_direction = ''INBOUND''
                      ),

                      json_agg(
                        DISTINCT linear_id ORDER BY linear_id
                      ) FILTER (
                        WHERE traversal_direction = ''OUTBOUND''
                      )
                    )::TEXT
                  ) AS label
                FROM %s
                GROUP BY 1, 2
            ) AS z
            USING ( node_id_b, wkb_geometry_b, label )
      ;
    '
  ),
  (
    4,
    10,
    '
      INSERT INTO %I (
        node_id_a,
        node_id_b,
        label,
        label_fields
      )
        SELECT DISTINCT
            x.node_id_a,
            x.node_id_b,
            x.label::JSONB,
            x.label_fields
          FROM (
            SELECT
                a.node_id AS node_id_a,
                a.wkb_geometry::TEXT AS wkb_geometry_a,

                b.node_id AS node_id_b,
                b.wkb_geometry::TEXT AS wkb_geometry_b,

                jsonb_build_array(''roadname'') AS label_fields,

                (
                  json_build_array(
                    json_agg(
                      DISTINCT roadname ORDER BY roadname
                    ) FILTER (
                      WHERE traversal_direction = ''INBOUND''
                    ),

                    json_agg(
                      DISTINCT roadname ORDER BY roadname
                    ) FILTER (
                      WHERE traversal_direction = ''OUTBOUND''
                    )
                  )::TEXT
                ) AS label

              FROM %s AS a
                INNER JOIN %s AS b
                  USING ( roadname, traversal_direction )
              WHERE (
                ( ABS( a.bearing - b.bearing ) <= ( 3 * %s ) /*degrees*/ )
                AND
                (
                  public.ST_Distance(
                    a.wkb_geometry::public.geography,
                    b.wkb_geometry::public.geography
                  ) <= ( 1.5 * %s ) /*meter*/
                )
              )
              GROUP BY 1, 2, 3, 4
              HAVING ( COUNT( DISTINCT roadname ) > 1 )
          ) AS x
            INNER JOIN (
              SELECT
                  node_id AS node_id_a,
                  wkb_geometry::TEXT AS wkb_geometry_a,

                  (
                    json_build_array(
                      json_agg(
                        DISTINCT roadname ORDER BY roadname
                      ) FILTER (
                        WHERE traversal_direction = ''INBOUND''
                      ),

                      json_agg(
                        DISTINCT roadname ORDER BY roadname
                      ) FILTER (
                        WHERE traversal_direction = ''OUTBOUND''
                      )
                    )::TEXT
                  ) AS label
                FROM %s
                GROUP BY 1, 2
            ) AS y
              USING ( node_id_a, wkb_geometry_a, label )
            INNER JOIN (
              SELECT
                  node_id AS node_id_b,
                  wkb_geometry::TEXT AS wkb_geometry_b,

                  (
                    json_build_array(
                      json_agg(
                        DISTINCT roadname ORDER BY roadname
                      ) FILTER (
                        WHERE traversal_direction = ''INBOUND''
                      ),

                      json_agg(
                        DISTINCT roadname ORDER BY roadname
                      ) FILTER (
                        WHERE traversal_direction = ''OUTBOUND''
                      )
                    )::TEXT
                  ) AS label
                FROM %s
                GROUP BY 1, 2
            ) AS z
            USING ( node_id_b, wkb_geometry_b, label )
      ;
    '
  ),
  (
    5,
    10,
    '
      INSERT INTO %I (
        node_id_a,
        node_id_b,
        label,
        label_fields
      )
        SELECT DISTINCT
            x.node_id_a,
            x.node_id_b,
            x.label::JSONB,
            x.label_fields
          FROM (
            SELECT
                a.node_id AS node_id_a,
                a.wkb_geometry::TEXT AS wkb_geometry_a,

                b.node_id AS node_id_b,
                b.wkb_geometry::TEXT AS wkb_geometry_b,

                jsonb_build_array(''roadnumber'') AS label_fields,

                (
                  json_build_array(
                    json_agg(
                      DISTINCT roadnumber ORDER BY roadnumber
                    ) FILTER (
                      WHERE traversal_direction = ''INBOUND''
                    ),

                    json_agg(
                      DISTINCT roadnumber ORDER BY roadnumber
                    ) FILTER (
                      WHERE traversal_direction = ''OUTBOUND''
                    )
                  )::TEXT
                ) AS label

              FROM %s AS a
                INNER JOIN %s AS b
                  USING ( roadnumber, traversal_direction )
              WHERE (
                ( ABS( a.bearing - b.bearing ) <= ( 3 * %s ) /*degrees*/ )
                AND
                (
                  public.ST_Distance(
                    a.wkb_geometry::public.geography,
                    b.wkb_geometry::public.geography
                  ) <= ( 1.5 * %s ) /*meter*/
                )
              )
              GROUP BY 1, 2, 3, 4
              HAVING ( COUNT( DISTINCT roadnumber ) > 1 )
          ) AS x
            INNER JOIN (
              SELECT
                  node_id AS node_id_a,
                  wkb_geometry::TEXT AS wkb_geometry_a,

                  (
                    json_build_array(
                      json_agg(
                        DISTINCT roadnumber ORDER BY roadnumber
                      ) FILTER (
                        WHERE traversal_direction = ''INBOUND''
                      ),

                      json_agg(
                        DISTINCT roadnumber ORDER BY roadnumber
                      ) FILTER (
                        WHERE traversal_direction = ''OUTBOUND''
                      )
                    )::TEXT
                  ) AS label
                FROM %s
                GROUP BY 1, 2
            ) AS y
              USING ( node_id_a, wkb_geometry_a, label )
            INNER JOIN (
              SELECT
                  node_id AS node_id_b,
                  wkb_geometry::TEXT AS wkb_geometry_b,

                  (
                    json_build_array(
                      json_agg(
                        DISTINCT roadnumber ORDER BY roadnumber
                      ) FILTER (
                        WHERE traversal_direction = ''INBOUND''
                      ),

                      json_agg(
                        DISTINCT roadnumber ORDER BY roadnumber
                      ) FILTER (
                        WHERE traversal_direction = ''OUTBOUND''
                      )
                    )::TEXT
                  ) AS label
                FROM %s
                GROUP BY 1, 2
            ) AS z
            USING ( node_id_b, wkb_geometry_b, label )
      ;
    '
  ),
  (
    6,
    10,
    '
      INSERT INTO %I (
        node_id_a,
        node_id_b,
        label,
        label_fields
      )
        SELECT
            a.node_id AS node_id_a,
            b.node_id AS node_id_b,

            jsonb_agg(DISTINCT linear_id ORDER BY linear_id) AS label,
            jsonb_build_array(''linear_id'') AS label_fields

          FROM %s AS a
            INNER JOIN %s AS b
              USING (linear_id)
          WHERE (
            ( ABS( a.bearing - b.bearing ) <= ( 1.5 * %s ) /*degrees*/ )
            AND
            (
              public.ST_Distance(
                a.wkb_geometry::public.geography,
                b.wkb_geometry::public.geography
              ) <= ( 0.75 * %s ) /*meter*/
            )
          )
          GROUP BY 1,2
          HAVING COUNT(DISTINCT linear_id) > 1
          -- We need these placeholders so this query fits the FORMAT pattern: %s %s
      ;
    '
  )
;

CREATE OR REPLACE FUNCTION npmrds_network_spatial_analysis.incident_edges_conformal_matches (
  incident_edge_metadata_a REGCLASS,
  incident_edge_metadata_b REGCLASS
)
  RETURNS TABLE (
    node_id_a         INTEGER,
    node_id_b         INTEGER,
    label             JSONB,
    label_fields      JSONB,
    conformal_level   TEXT
  )

  LANGUAGE plpgsql AS

  $func$
    DECLARE
      tmp_result_matches_table  CONSTANT TEXT := 'tmp_' || public.uuid_generate_v4()::TEXT ;
      tmp_matches_table_name    CONSTANT TEXT := 'tmp_' || public.uuid_generate_v4()::TEXT ;

      match_query_rec RECORD ;

      matches_count INTEGER ;
    BEGIN

      EXECUTE FORMAT(
        '
          CREATE TEMPORARY TABLE %I (
            node_id_a         INTEGER NOT NULL UNIQUE,   -- node from map a
            node_id_b         INTEGER NOT NULL UNIQUE,   -- node from map b
            label             JSONB,
            label_fields      JSONB,
            conformal_level   TEXT NOT NULL,

            PRIMARY KEY (node_id_a, node_id_b)
          ) ON COMMIT DROP ;
        ', tmp_result_matches_table
      ) ;

      FOR match_query_rec IN
          SELECT
              *
            FROM npmrds_network_spatial_analysis.incident_edges_conformal_match_queries
            --  WHERE match_level = 4
        LOOP

          FOR spatial_tolerance IN 0..(match_query_rec."max_spatial_tolerance")
            LOOP

              EXECUTE FORMAT(
                '
                  CREATE TEMPORARY TABLE %I (
                    node_id_a       INTEGER NOT NULL,    -- node from map a
                    node_id_b       INTEGER NOT NULL,     -- node from map b
                    label           JSONB,
                    label_fields    JSONB
                  ) ;
                ', tmp_matches_table_name
              ) ;

              EXECUTE FORMAT(
                match_query_rec."match_query",
                -- INSERT INTO
                tmp_matches_table_name,

                --  FROM a
                incident_edge_metadata_a,
                --  INNER JOIN b
                incident_edge_metadata_b,
                --  ON
                --    degrees
                spatial_tolerance,
                --    meters
                spatial_tolerance,

                --  INNER JOIN y
                --    FROM
                incident_edge_metadata_a,
                --  INNER JOIN z
                --    FROM
                incident_edge_metadata_b
            ) ;

RAISE NOTICE 'match_level %:', match_query_rec."match_level" || '.' || LPAD(spatial_tolerance::TEXT, 3, '0');

EXECUTE FORMAT('SELECT COUNT(1) FROM %I', tmp_matches_table_name) INTO matches_count ;
RAISE NOTICE '    found:   %', matches_count ;

              EXECUTE FORMAT(
                '
                  DELETE FROM %I
                    WHERE (
                      node_id_a IN (
                        -- The matching must be unique for node_id_a
                        SELECT
                            node_id_a
                          FROM %I
                          GROUP BY 1
                          HAVING ( COUNT(1) > 1 )
                        UNION
                        SELECT
                            node_id_a
                          FROM %I
                      )
                    )
                  ;
                ', 
                  tmp_matches_table_name,
                  tmp_matches_table_name,
                  tmp_result_matches_table
              ) ;

              EXECUTE FORMAT(
                '
                  DELETE FROM %I
                    WHERE (
                      -- The matching must be unique for node_id_b
                      node_id_b IN (
                        SELECT
                            node_id_b
                          FROM %I
                          GROUP BY 1
                          HAVING ( COUNT(1) > 1 )
                        UNION
                        SELECT
                            node_id_b
                          FROM %I
                      )
                    )
                  ;
                ', 
                  tmp_matches_table_name,
                  tmp_matches_table_name,
                  tmp_result_matches_table
              ) ;

EXECUTE FORMAT('SELECT COUNT(1) FROM %I', tmp_matches_table_name) INTO matches_count ;
RAISE NOTICE '    keeping: %', matches_count ;

              EXECUTE FORMAT(
                '
                  INSERT INTO %I (
                    node_id_a,
                    node_id_b,
                    label,
                    label_fields,
                    conformal_level
                  )
                    SELECT
                        node_id_a,
                        node_id_b,
                        label,
                        label_fields,
                        %s AS conformal_level
                      FROM %I
                  ;
                ', 
                  tmp_result_matches_table,
                  match_query_rec."match_level" || '.' || LPAD(spatial_tolerance::TEXT, 3, '0'),
                  tmp_matches_table_name
              ) ;

              EXECUTE FORMAT('DROP TABLE %I ;', tmp_matches_table_name) ;

          END LOOP ;

        END LOOP ;

        RETURN QUERY EXECUTE FORMAT(
          '
            SELECT
                node_id_a,
                node_id_b,
                label,
                label_fields,
                conformal_level
              FROM %I
          ', tmp_result_matches_table
        ) ;

    END ;

  $func$
;
