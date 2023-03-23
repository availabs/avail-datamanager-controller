CREATE SCHEMA IF NOT EXISTS data_manager ;
CREATE SCHEMA IF NOT EXISTS data_manager;

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

-- https://til.cybertec-postgresql.com/post/2019-09-02-Postgres-Constraint-Naming-Convention/
CREATE TABLE IF NOT EXISTS data_manager.sources (
  source_id             SERIAL PRIMARY KEY,
  -- The next line creates the "sources_name_key" UNIQUE CONSTRAINT using the naming conventions.
  name                  TEXT NOT NULL CHECK (char_length(name) <= 32) UNIQUE,
  update_interval       TEXT,
  category              TEXT[],
  description           TEXT,
  statistics            JSONB,
  metadata              JSONB,
  categories            JSONB,
  type                  TEXT,
  display_name          TEXT,

  source_dependencies   INTEGER[],

  user_id               INTEGER,

  _created_timestamp    TIMESTAMP NOT NULL DEFAULT NOW(),
  _modified_timestamp   TIMESTAMP NOT NULL DEFAULT NOW()
) ;

-- START data_manager.sources Migrations

ALTER TABLE data_manager.sources
  ADD COLUMN IF NOT EXISTS source_dependencies INTEGER[]
;

-- END data_manager.sources Migrations

CREATE TABLE IF NOT EXISTS data_manager.views (
  view_id                 SERIAL PRIMARY KEY,

  source_id               INTEGER NOT NULL REFERENCES data_manager.sources (source_id) ON DELETE CASCADE,

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

  -- NOTE: FOREIGN KEY CONSTRAINTs added in create_dama_etl_context_and_events_tables.sql
  etl_context_id          INTEGER,

  view_dependencies       INTEGER[],

  active_start_timestamp  TIMESTAMP,
  active_end_timestamp    TIMESTAMP,

  _created_timestamp      TIMESTAMP NOT NULL DEFAULT NOW(),
  _modified_timestamp     TIMESTAMP NOT NULL DEFAULT NOW()
) ;

-- START data_manager.views Migrations

ALTER TABLE data_manager.views
  ADD COLUMN IF NOT EXISTS view_dependencies INTEGER[]
;

-- END data_manager.views Migrations

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

-- Utility Functions

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

CREATE OR REPLACE VIEW data_manager.views_primary_keys
  AS
    SELECT
        v.view_id,
        MAX(v.source_id) AS source_id,
        MAX(v.table_schema) AS table_schema,
        MAX(v.table_name) AS table_name,
        array_agg(a.attname ORDER BY t.attridx) AS primary_key_cols,
        array_agg(x.typname ORDER BY t.attridx) AS primary_key_cols_types
      FROM pg_catalog.pg_index AS i
        CROSS JOIN LATERAL UNNEST(i.indkey) AS t(attridx)
        INNER JOIN pg_catalog.pg_attribute AS a
          ON (
            ( i.indisprimary )
            AND
            ( a.attrelid = i.indrelid )
            AND
            ( a.attnum = t.attridx )
          )
        INNER JOIN pg_catalog.pg_type AS x
          ON (
            ( a.atttypid = x.oid )
          )
        INNER JOIN data_manager.views AS v
          ON (
            ( table_schema IS NOT NULL )
            AND
            ( table_name IS NOT NULL )
            AND
            ( i.indrelid = ( format('%I.%I', table_schema, table_name) )::regclass )
          )
      GROUP BY 1
;
