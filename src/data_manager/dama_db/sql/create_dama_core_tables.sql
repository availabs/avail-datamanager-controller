CREATE SCHEMA IF NOT EXISTS data_manager ;
CREATE SCHEMA IF NOT EXISTS _data_manager_admin;


CREATE TABLE IF NOT EXISTS data_manager.sources (
  id                SERIAL PRIMARY KEY,
  name              TEXT NOT NULL UNIQUE,
  update_interval   TEXT,
  category          TEXT[],
  description       TEXT,
  statistics        JSONB,
  metadata          JSONB,
  categories        JSONB,
  type              TEXT,
  display_name      TEXT
) ;

CREATE TABLE IF NOT EXISTS data_manager.views (
  id                      SERIAL PRIMARY KEY,

  source_id               INTEGER NOT NULL REFERENCES data_manager.sources (id),

  data_type               TEXT,
  interval_version        TEXT, -- could be year, or year-month
  geography_version       TEXT, -- mostly 2 digit state codes, sometimes null
  version                 TEXT, -- default 1
  source_url              TEXT, -- external source url
  publisher               TEXT,
  data_table              TEXT, -- schema.table of internal destination
  table_schema            TEXT,
  table_name              TEXT,
  download_url            TEXT, -- url for client download
  tiles_url               TEXT, -- tiles
  start_date              DATE,
  end_date                DATE,
  last_updated            TIMESTAMP,
  statistics              JSONB,
  metadata                JSONB,

  root_etl_context_id     INTEGER,
  etl_context_id          INTEGER,

  _created_timestamp      TIMESTAMP NOT NULL DEFAULT NOW(),
  _modified_timestamp     TIMESTAMP NOT NULL DEFAULT NOW()
) ;
