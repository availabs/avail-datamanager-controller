CREATE SCHEMA IF NOT EXISTS _transcom_admin ;

CREATE OR REPLACE FUNCTION _transcom_admin.update_modified_timestamp_trigger_fn()
  RETURNS TRIGGER AS $trigger_fn$
    DECLARE
        old_as_json JSONB;
        new_as_json JSONB;
        column_name TEXT;

        is_modified BOOLEAN := FALSE;

    BEGIN
      -- https://stackoverflow.com/a/65359512/3970755
      old_as_json := to_jsonb(old) ;
      new_as_json := to_jsonb(new) ;

      -- _created_timestamp is immutable.
      NEW._created_timestamp = OLD._created_timestamp;

      -- _modified_timestamp only changes if data changes.
      FOR column_name IN SELECT jsonb_object_keys(old_as_json)
        LOOP
          IF ( column_name = '_modified_timestamp' )
            THEN
              CONTINUE;
          END IF ;

          IF (old_as_json->column_name <> new_as_json->column_name)
            THEN 
              NEW._modified_timestamp = NOW() ;
              RETURN NEW ;
          END IF;
      END LOOP;

      RETURN OLD ;
    END;
$trigger_fn$ LANGUAGE plpgsql ;


