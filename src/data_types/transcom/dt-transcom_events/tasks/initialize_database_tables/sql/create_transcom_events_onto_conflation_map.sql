CREATE SCHEMA IF NOT EXISTS transcom;

CREATE TABLE IF NOT EXISTS transcom.transcom_events_onto_conflation_map (
    event_id                      TEXT,
    year                          SMALLINT,
    conflation_way_id             BIGINT NOT NULL,
    conflation_node_id            BIGINT,
    osm_fwd                       SMALLINT,
    both_directions               SMALLINT,
    n                             SMALLINT,

    snap_pt_geom                  public.geometry(Point, 4326) NOT NULL,

    _modified_timestamp           TIMESTAMP NOT NULL DEFAULT NOW(),

    PRIMARY KEY (event_id, year)
  ) WITH (fillfactor=100, autovacuum_enabled=false)
;

