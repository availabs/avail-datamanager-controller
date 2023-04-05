CREATE SCHEMA IF NOT EXISTS _transcom_admin;

CREATE TABLE IF NOT EXISTS _transcom_admin.etl_control (
  id                    SERIAL PRIMARY  KEY,
  start_timestamp       TIMESTAMP NOT NULL,
  end_timestamp         TIMESTAMP,
  metadata              JSONB NOT NULL DEFAULT jsonb_build_object(),

  _created_timestamp    TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
  _modified_timestamp   TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
) ;

DROP TRIGGER IF EXISTS etl_control_update_trigger
  ON _transcom_admin.etl_control
;

CREATE TRIGGER etl_control_update_trigger
 BEFORE
   UPDATE ON _transcom_admin.etl_control
 FOR EACH ROW
   EXECUTE PROCEDURE _transcom_admin.update_modified_timestamp_trigger_fn()
;

