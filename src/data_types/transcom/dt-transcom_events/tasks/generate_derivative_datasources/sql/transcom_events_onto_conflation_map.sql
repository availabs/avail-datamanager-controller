CREATE TABLE IF NOT EXISTS :staging_schema.transcom_events_onto_conflation_map_:conflation_version (
  event_id                      TEXT,
  year                          SMALLINT,

  conflation_way_id             BIGINT NOT NULL,
  conflation_node_id            BIGINT,
  osm_fwd                       SMALLINT,
  both_directions               SMALLINT,
  n                             SMALLINT,

  snap_pt_geom                  public.geometry(Point, 4326) NOT NULL,

  PRIMARY KEY (event_id, year)
) WITH (fillfactor=100, autovacuum_enabled=false)
;


CREATE TABLE :staging_schema.event_to_cways_knn_:event_year (
  event_id            TEXT,
  c_way_id            BIGINT,
  snap_dist           DOUBLE PRECISION,
  snap_pt_geom        public.geometry(Geometry,4326),
  PRIMARY KEY (event_id, c_way_id)
) WITH (fillfactor=100)
;

CREATE INDEX event_to_cways_knn_:event_year_idx -- placeholder is :event_year, not :event_year_idx
  ON :staging_schema.event_to_cways_knn_:event_year (c_way_id)
;

INSERT INTO :staging_schema.event_to_cways_knn_:event_year (
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
    FROM :staging_schema.transcom_events_gis_optimized AS a
      INNER JOIN LATERAL (
        SELECT
          id AS c_way_id,
          ST_Distance(
            GEOGRAPHY(a.point_geom),
            GEOGRAPHY(x.wkb_geometry)
          ) AS snap_dist,
          ST_ClosestPoint(x.wkb_geometry, a.point_geom) AS snap_pt_geom
        FROM conflation.conflation_map_:event_year_:conflation_version AS x
        WHERE ( x.n < 7 )
        ORDER BY ( a.point_geom <-> x.wkb_geometry ) ASC
        LIMIT :knn_k -- The K of the KNN
      ) AS b ON TRUE
    WHERE (
      ( a.year = :event_year )
      AND
      ( snap_dist < 1000 )
    )
;

CLUSTER :staging_schema.event_to_cways_knn_:event_year
  USING event_to_cways_knn_:event_year_idx -- placeholder is :event_year, not :event_year_idx
;

ANALYZE :staging_schema.event_to_cways_knn_:event_year ;

INSERT INTO :staging_schema.transcom_events_onto_conflation_map_:conflation_version (
  event_id,
  year,
  conflation_way_id,
  osm_fwd,
  both_directions,
  n,
  snap_pt_geom
)
  SELECT
      event_id,
      :event_year AS year,
      conflation_way_id,
      osm_fwd,
      both_directions,
      n,
      snap_pt_geom
    FROM (
      SELECT
          event_id,
          conflation_way_id,
          osm_fwd,
          both_directions,
          n,
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
              c.id AS conflation_way_id,
              c.osm_fwd,
              ( a.direction = 'both directions' )::INTEGER AS both_directions,
              c.n,
              b.snap_pt_geom,
              b.snap_dist,
              (
                COALESCE(d.direction, 'NONE') =
                  CASE
                    WHEN a.direction = 'northbound'  THEN 'N'
                    WHEN a.direction = 'southbound'  THEN 'S'
                    WHEN a.direction = 'eastbound'   THEN 'E'
                    WHEN a.direction = 'westbound'   THEN 'W'
                    ELSE 'NONE'
                  END
              ) AS is_same_dir
            FROM :staging_schema.transcom_events_gis_optimized AS a
              INNER JOIN :staging_schema.event_to_cways_knn_:event_year  AS b
                USING (event_id)
              INNER JOIN conflation.conflation_map_:event_year_:conflation_version AS c
                ON ( b.c_way_id = c.id )
              LEFT OUTER JOIN ny.tmc_metadata_:event_year AS d
                USING (tmc)
            WHERE ( a.year = :event_year )
        ) AS x
      ) AS y
      WHERE ( rownum = 1 )
;


UPDATE
    :staging_schema.transcom_events_onto_conflation_map_:conflation_version AS t1
  SET
    conflation_node_id = t2.conflation_node_id
  FROM (

    SELECT
        a.event_id,
        a.year,
        b.c_node_id AS conflation_node_id

      FROM :staging_schema.transcom_events_onto_conflation_map_:conflation_version AS a
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
          FROM conflation.conflation_map_:event_year_:conflation_version AS x
            INNER JOIN LATERAL (                      -- node ids for each conflation map way
              SELECT
                  UNNEST(t.node_ids)  AS c_node_id    
                FROM conflation.conflation_map_:event_year_ways_:conflation_version AS t
                WHERE ( x.id = t.id )
            ) AS y ON TRUE
              INNER JOIN conflation.conflation_map_:event_year_nodes_:conflation_version AS z
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
;

CLUSTER :staging_schema.transcom_events_onto_conflation_map_:conflation_version
  USING transcom_events_onto_conflation_map_:conflation_version_pkey ; -- placeholder is :conflation_version

ANALYZE :staging_schema.transcom_events_onto_conflation_map_:conflation_version ;
