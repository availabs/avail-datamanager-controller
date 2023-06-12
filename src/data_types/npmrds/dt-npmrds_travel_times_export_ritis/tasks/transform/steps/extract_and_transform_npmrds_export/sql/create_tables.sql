BEGIN ;

-- This table is used to make sure that the LEFT OUTER JOINs below
--   include all (tmc_code, measurement_tstamp) pairs found in the data.
CREATE TABLE all_tmc_timestamp_pairs (
  tmc_code              TEXT NOT NULL,
  measurement_tstamp    TEXT NOT NULL,

  PRIMARY KEY ( tmc_code, measurement_tstamp )
) WITHOUT ROWID ;

CREATE TABLE all_vehicles (
  tmc_code              TEXT NOT NULL,
  measurement_tstamp    TEXT NOT NULL,
  speed                 TEXT NOT NULL,
  average_speed         TEXT,
  reference_speed       TEXT,
  travel_time_seconds   TEXT NOT NULL,
  data_density          TEXT,

  PRIMARY KEY ( tmc_code, measurement_tstamp )
) WITHOUT ROWID ;

CREATE TABLE passenger_vehicles (
  tmc_code              TEXT NOT NULL,
  measurement_tstamp    TEXT NOT NULL,
  speed                 TEXT NOT NULL,
  average_speed         TEXT,
  reference_speed       TEXT,
  travel_time_seconds   TEXT NOT NULL,
  data_density          TEXT,

  PRIMARY KEY ( tmc_code, measurement_tstamp )
) WITHOUT ROWID ;

CREATE TABLE trucks (
  tmc_code              TEXT NOT NULL,
  measurement_tstamp    TEXT NOT NULL,
  speed                 TEXT NOT NULL,
  average_speed         TEXT,
  reference_speed       TEXT,
  travel_time_seconds   TEXT NOT NULL,
  data_density          TEXT,

  PRIMARY KEY ( tmc_code, measurement_tstamp )
) WITHOUT ROWID ;

-- Should contain a single row. TODO: Add TRIGGER to enforce.
CREATE TABLE metadata (
  name                TEXT PRIMARY KEY,
  state               TEXT NOT NULL,
  year                INTEGER NOT NULL,

  start_date          TEXT NOT NULL
                        CONSTRAINT start_date_format_chk
                        CHECK(start_date IS strftime('%Y-%m-%d', start_date)),

  end_date            TEXT NOT NULL
                        CONSTRAINT data_end_date_format_chk
                        CHECK(end_date IS strftime('%Y-%m-%d', end_date)),

  is_expanded         BOOLEAN NOT NULL,

  is_complete_month   BOOLEAN NOT NULL,
  is_complete_week    BOOLEAN NOT NULL,

  download_timestamp  DATE
                        CONSTRAINT download_timestamp_format_chk
                        CHECK(download_timestamp IS strftime('%Y-%m-%dT%H:%M:%S', download_timestamp))
) ;

CREATE TABLE npmrds_travel_times (
  tmc                                 TEXT NOT NULL,

  date                                TEXT NOT NULL
                                      CONSTRAINT date_format_chk
                                      CHECK(
                                        strftime(
                                          '%Y%m%d',
                                          substr(date, 1, 4)
                                            || '-'
                                            || substr(date, 5, 2)
                                            || '-'
                                            || substr(date, 7, 2)
                                        ) = date
                                      ),

  epoch                               INTEGER NOT NULL,

  travel_time_all_vehicles            TEXT,
  travel_time_passenger_vehicles      TEXT,
  travel_time_freight_trucks          TEXT,

  data_density_all_vehicles           TEXT,
  data_density_passenger_vehicles     TEXT,
  data_density_freight_trucks         TEXT,

  PRIMARY KEY ( tmc, date, epoch )
) WITHOUT ROWID ;

COMMIT ;
