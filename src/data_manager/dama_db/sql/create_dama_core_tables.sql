CREATE SCHEMA IF NOT EXISTS data_manager ;
CREATE SCHEMA IF NOT EXISTS _data_manager_admin;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS data_manager.database_id
  AS
    SELECT
        uuid,
        regexp_replace(uuid::TEXT, '[^0-9a-z]', '', 'ig') AS id,
        SUBSTRING(uuid::TEXT FROM 1 FOR 8) AS short_id
      FROM (
        SELECT
            uuid_generate_v4() AS uuid
      ) AS t
;

CREATE TABLE IF NOT EXISTS data_manager.sources (
  source_id             SERIAL PRIMARY KEY,
  name                  TEXT NOT NULL CHECK (char_length(name) <= 32) UNIQUE,
  update_interval       TEXT,
  category              TEXT[],
  description           TEXT,
  statistics            JSONB,
  metadata              JSONB,
  categories            JSONB,
  type                  TEXT,
  display_name          TEXT,

  user_id               INTEGER,

  _created_timestamp    TIMESTAMP NOT NULL DEFAULT NOW(),
  _modified_timestamp   TIMESTAMP NOT NULL DEFAULT NOW()
) ;

CREATE TABLE IF NOT EXISTS data_manager.views (
  view_id                 SERIAL PRIMARY KEY,

  source_id               INTEGER NOT NULL REFERENCES data_manager.sources (source_id),

  data_type               TEXT,
  interval_version        TEXT, -- could be year, or year-month
  geography_version       TEXT, -- mostly 2 digit state codes, sometimes null
  version                 TEXT, -- default 1
  source_url              TEXT, -- external source url
  publisher               TEXT,
  table_schema            TEXT,
  table_name              TEXT,
  data_table              TEXT,
  download_url            TEXT, -- url for client download
  tiles_url               TEXT, -- tiles
  start_date              DATE,
  end_date                DATE,
  last_updated            TIMESTAMP,
  statistics              JSONB,
  metadata                JSONB,

  user_id                 INTEGER,

  root_etl_context_id     INTEGER,
  etl_context_id          INTEGER,

  _created_timestamp      TIMESTAMP NOT NULL DEFAULT NOW(),
  _modified_timestamp     TIMESTAMP NOT NULL DEFAULT NOW()
) ;

CREATE OR REPLACE FUNCTION data_manager.dama_update_modified_timestamp_fn()
  RETURNS TRIGGER
  LANGUAGE plpgsql
  AS
  $$
    BEGIN
      NEW._modified_timestamp = NOW() ;
      RETURN NEW ;
    END ;
  $$
;

CREATE OR REPLACE FUNCTION data_manager.dama_database_id_table_is_immutable()
  RETURNS TRIGGER
  LANGUAGE plpgsql
  AS
  $$
    BEGIN
      RAISE EXCEPTION 'The data_manager.database_id TABLE is immutable.' ;
    END ;
  $$
;

DO
  LANGUAGE plpgsql
  $$
    DECLARE
      trigger_exists  BOOLEAN ;

    BEGIN
      SELECT EXISTS (
        SELECT
            1
          FROM information_schema.triggers
          WHERE (
            ( trigger_schema = 'data_manager' )
            AND
            ( trigger_name = 'database_id_no_insert' )
          )
      ) INTO trigger_exists ;

      IF NOT trigger_exists
        THEN

          CREATE TRIGGER database_id_no_insert
            BEFORE INSERT
            ON data_manager.database_id
            EXECUTE FUNCTION data_manager.dama_database_id_table_is_immutable()
          ;

          CREATE TRIGGER database_id_no_update
            BEFORE UPDATE
            ON data_manager.database_id
            EXECUTE FUNCTION data_manager.dama_database_id_table_is_immutable()
          ;

          CREATE TRIGGER database_id_no_delete
            BEFORE DELETE
            ON data_manager.database_id
            EXECUTE FUNCTION data_manager.dama_database_id_table_is_immutable()
          ;

          CREATE TRIGGER database_id_no_trucate
            BEFORE TRUNCATE
            ON data_manager.database_id
            EXECUTE FUNCTION data_manager.dama_database_id_table_is_immutable()
          ;

      END IF ;

      SELECT EXISTS (
        SELECT
            1
          FROM information_schema.triggers
          WHERE (
            ( trigger_schema = 'data_manager' )
            AND
            ( trigger_name = 'dama_source_modified_timestamp_trigger' )
          )
      ) INTO trigger_exists ;

      IF NOT trigger_exists
        THEN
          CREATE TRIGGER dama_source_modified_timestamp_trigger
            BEFORE UPDATE
            ON data_manager.sources
            FOR EACH ROW
            EXECUTE PROCEDURE data_manager.dama_update_modified_timestamp_fn();
      END IF ;

      SELECT EXISTS (
        SELECT
            1
          FROM information_schema.triggers
          WHERE (
            ( trigger_schema = 'data_manager' )
            AND
            ( trigger_name = 'dama_view_modified_timestamp_trigger' )
          )
      ) INTO trigger_exists ;

      IF NOT trigger_exists
        THEN
          CREATE TRIGGER dama_view_modified_timestamp_trigger
            BEFORE UPDATE
            ON data_manager.views
            FOR EACH ROW
            EXECUTE PROCEDURE data_manager.dama_update_modified_timestamp_fn();
      END IF ;
    END
  $$
;

CREATE OR REPLACE FUNCTION data_manager.dama_db_uuid()
  RETURNS UUID
  LANGUAGE SQL
  IMMUTABLE
  AS
  $$
    SELECT uuid
      FROM data_manager.database_id
  $$
;

CREATE OR REPLACE FUNCTION data_manager.dama_db_id()
  RETURNS TEXT
  LANGUAGE SQL
  IMMUTABLE
  AS
  $$
    SELECT id
      FROM data_manager.database_id
  $$
;

CREATE OR REPLACE FUNCTION data_manager.dama_db_short_id()
  RETURNS TEXT
  LANGUAGE SQL
  IMMUTABLE
  AS
  $$
    SELECT short_id
      FROM data_manager.database_id
  $$
;
