CREATE SCHEMA IF NOT EXISTS _transcom_admin;

CREATE TABLE IF NOT EXISTS _transcom_admin.transcom_events_expanded (
  event_id                                      TEXT PRIMARY KEY,
  event_class                                   TEXT,
  reporting_organization                        TEXT,
  start_date_time                               TIMESTAMP,
  end_date_time                                 TEXT, -- For all observed data, value was null
  last_updatedate                               TIMESTAMP,
  close_date                                    TIMESTAMP,
  estimated_duration_mins                       INTEGER,
  event_duration                                TEXT,
  facility                                      TEXT,
  event_type                                    TEXT,
  lanes_total_count                             SMALLINT,
  lanes_affected_count                          SMALLINT,
  lanes_detail                                  TEXT,
  lanes_status                                  TEXT,
  description                                   TEXT,
  direction                                     TEXT,
  county                                        TEXT,
  city                                          TEXT,
  city_article                                  TEXT,
  primary_city                                  TEXT, -- For all observed data, value was null
  secondary_city                                TEXT,
  point_lat                                     NUMERIC,
  point_long                                    NUMERIC,
  location_article                              TEXT,
  primary_marker                                REAL,
  secondary_marker                              REAL,
  primary_location                              TEXT,
  secondary_location                            TEXT,
  state                                         TEXT,
  region_closed                                 BOOLEAN,
  point_datum                                   TEXT,
  marker_units                                  TEXT, -- For all observed data, value was null
  marker_article                                TEXT,
  summary_description                           TEXT,
  eventstatus                                   TEXT,
  is_highway                                    BOOLEAN,
  icon_file                                     TEXT,
  start_incident_occured                        TIMESTAMP,
  started_at_date_time_comment                  TEXT, -- For all observed data, value was null
  incident_reported                             TIMESTAMP,
  incident_reported_comment                     TEXT, -- For all observed data, value was null
  incident_verified                             TIMESTAMP,
  incident_verified_comment                     TEXT, -- For all observed data, value was null
  response_identified_and_dispatched            TIMESTAMP,
  response_identified_and_dispatched_comment    TEXT, -- For all observed data, value was null
  response_arrives_on_scene                     TIMESTAMP,
  response_arrives_on_scene_comment             TEXT,
  end_all_lanes_open_to_traffic                 TIMESTAMP,
  ended_at_date_time_comment                    TEXT, -- For all observed data, value was null
  response_departs_scene                        TIMESTAMP,
  response_departs_scene_comment                TEXT, -- For all observed data, value was null
  time_to_return_to_normal_flow                 TIMESTAMP,
  time_to_return_to_normal_flow_comment         TEXT, -- For all observed data, value was null
  no_of_vehicle_involved                        TEXT,
  secondary_event                               BOOLEAN,
  secondary_event_types                         TEXT, -- For all observed data, value was null
  secondary_involvements                        TEXT, -- For all observed data, value was null
  within_work_zone                              BOOLEAN,
  truck_commercial_vehicle_involved             BOOLEAN,
  shoulder_available                            BOOLEAN,
  injury_involved                               BOOLEAN,
  fatality_involved                             BOOLEAN,
  maintance_crew_involved                       BOOLEAN,
  roadway_clearance                             TEXT,
  incident_clearance                            TEXT,
  time_to_return_to_normal_flow_duration        TEXT,
  duration                                      TEXT,
  associated_impact_ids                         TEXT,
  secondary_event_ids                           TEXT,
  is_transit                                    BOOLEAN,
  is_shoulder_lane                              BOOLEAN,
  is_toll_lane                                  BOOLEAN,
  lanes_affected_detail                         TEXT,
  to_facility                                   TEXT,
  to_state                                      TEXT,
  to_direction                                  TEXT,
  fatality_involved_associated_event_id         BOOLEAN,
  with_in_work_zone_associated_event_id         TEXT,
  to_lat                                        NUMERIC,
  to_lon                                        NUMERIC,
  primary_direction                             TEXT,
  secondary_direction                           TEXT,
  is_both_direction                             BOOLEAN,
  secondary_lanes_affected_count                SMALLINT,
  secondary_lanes_detail                        TEXT,
  secondary_lanes_status                        TEXT,
  secondary_lanes_total_count                   SMALLINT,
  secondary_lanes_affected_detail               TEXT,
  event_location_latitude                       REAL,
  event_location_longitude                      REAL,
  tripcnt                                       BOOLEAN,
  tmclist                                       TEXT,
  recoverytime                                  SMALLINT,
  year                                          SMALLINT,
  datasource                                    BOOLEAN,
  datasourcevalue                               TEXT, -- For all observed data, value was null
  day_of_week                                   SMALLINT,
  tmc_geometry                                  TEXT,

  _created_timestamp                            TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
  _modified_timestamp                           TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
) WITH (fillfactor=100, autovacuum_enabled=false) ;

DO
  LANGUAGE plpgsql
  $$
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM _transcom_admin.transcom_events_expanded)
        THEN

          DROP TRIGGER IF EXISTS transcom_events_expanded_update_trigger
            ON _transcom_admin.transcom_events_expanded
          ;

          CREATE TRIGGER transcom_events_expanded_update_trigger
           BEFORE
             UPDATE ON _transcom_admin.transcom_events_expanded
           FOR EACH ROW
             EXECUTE PROCEDURE _transcom_admin.update_modified_timestamp_trigger_fn()
          ;
          -- ===== Indexes =====

          CREATE INDEX IF NOT EXISTS transcom_events_expanded_date_idx
            ON _transcom_admin.transcom_events_expanded (start_date_time, end_date_time)
          ;

          CREATE INDEX IF NOT EXISTS transcom_events_expanded_year_idx
            ON _transcom_admin.transcom_events_expanded (date_part('year'::text, start_date_time))
          ;

          -- point_geom will be in the VIEW
          CREATE INDEX IF NOT EXISTS transcom_events_expanded_geom_idx
            ON _transcom_admin.transcom_events_expanded
              USING GIST (
                public.ST_Transform(
                  public.ST_SetSRID(
                    public.ST_MakePoint(
                      point_long,
                      point_lat
                    ),
                    4269 -- NAD83 -- EPSG:4269
                  ),
                  4326  -- EPSG:4326
                )
              )
          ;

          CLUSTER _transcom_admin.transcom_events_expanded
            USING transcom_events_expanded_pkey;
      END IF ;
    END ;
  $$
;

CREATE OR REPLACE VIEW _transcom_admin.transcom_events_expanded_view
  AS
    SELECT
        *,
        string_to_array(tmclist, ',') AS tmcs_arr,

        (
          CASE
            WHEN ( event_duration ~ '^\d{1,} - [0-9:]{1,}$' )
              THEN regexp_replace(event_duration, '-', 'days')
              ELSE NULL
            END
        )::INTERVAL AS event_interval,

        public.ST_Transform(
          public.ST_SetSRID(
            public.ST_MakePoint(
              point_long,
              point_lat
            ),
            4269 -- NAD83 -- EPSG:4269
          ),
          4326  -- EPSG:4326
        ) AS point_geom

      FROM _transcom_admin.transcom_events_expanded
;
