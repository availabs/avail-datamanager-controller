CREATE SCHEMA IF NOT EXISTS _transcom_admin ;

CREATE TABLE IF NOT EXISTS _transcom_admin.transcom_event_administative_geographies (
    event_id                                        TEXT PRIMARY KEY,
    state_name                                      TEXT NOT NULL,
    state_code                                      TEXT NOT NULL,

    region_name                                     TEXT,
    region_code                                     TEXT,

    county_name                                     TEXT NOT NULL,
    county_code                                     TEXT NOT NULL,

    mpo_name                                        TEXT,
    mpo_code                                        TEXT,

    ua_name                                         TEXT,
    ua_code                                         TEXT
  ) WITH (fillfactor=100, autovacuum_enabled=off)
;

DO
  LANGUAGE plpgsql
  $$
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM _transcom_admin.transcom_event_administative_geographies)
        THEN
          CLUSTER _transcom_admin.transcom_event_administative_geographies
            USING transcom_event_administative_geographies_pkey;
      END IF ;
    END ;
  $$
;
