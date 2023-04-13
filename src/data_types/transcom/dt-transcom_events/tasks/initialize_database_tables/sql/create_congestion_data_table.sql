CREATE TABLE IF NOT EXISTS _transcom_admin.transcom_event_congestion_data (
  event_id          TEXT PRIMARY KEY,
  congestion_data   JSONB
) WITH (fillfactor=100, autovacuum_enabled=false) ;

DO
  LANGUAGE plpgsql
  $$
    BEGIN
      -- If the table is empty, we execute CLUSTER to establish the clustering INDEX.
      IF NOT EXISTS (SELECT 1 FROM _transcom_admin.transcom_event_congestion_data)
        THEN
          CLUSTER _transcom_admin.transcom_event_congestion_data
            USING transcom_event_congestion_data_pkey;
      END IF ;
    END ;
  $$
;
