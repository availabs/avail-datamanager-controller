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
) WITH (fillfactor=100, autovacuum_enabled=false) ;
