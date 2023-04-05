CREATE SCHEMA IF NOT EXISTS transcom;

CREATE TABLE IF NOT EXISTS transcom.nysdot_transcom_event_classifications (
  event_type                        TEXT PRIMARY KEY,
  display_in_incident_dashboard     BOOLEAN,
  general_category                  TEXT,
  sub_category                      TEXT,
  detailed_category                 TEXT,
  waze_category                     TEXT,
  display_if_lane_closure           BOOLEAN,
  duration_accurate                 TEXT,

  _created_timestamp                TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
  _modified_timestamp               TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
) WITH (fillfactor=100, autovacuum_enabled=false);

DROP TRIGGER IF EXISTS nysdot_transcom_event_classifications_update_trigger
  ON transcom.nysdot_transcom_event_classifications
;

CREATE TRIGGER nysdot_transcom_event_classifications_update_trigger
 BEFORE
   UPDATE ON transcom.nysdot_transcom_event_classifications
 FOR EACH ROW
   EXECUTE PROCEDURE _transcom_admin.update_modified_timestamp_trigger_fn()
;

CLUSTER transcom.nysdot_transcom_event_classifications
  USING nysdot_transcom_event_classifications_pkey
;
