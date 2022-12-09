--  After preparing npmrds_production by
--
--    1. executing the preparation.sql script
--    2. running dama_db.runDatabaseInitializationDDL
--
--  the following will populate the data in the new tables.

BEGIN ;

INSERT INTO data_manager.sources (
  source_id,
  name,
  update_interval,
  category,
  description,
  statistics,
  metadata,
  categories,
  type,
  display_name
)
  SELECT
      id,
      trim(
        regexp_replace(
          regexp_replace(
            name,
            '^.*/',
            ''
          ),
          '^.*:',
          ''
        )
      ) AS name,
      update_interval,
      category,
      description,
      statistics,
      metadata,
      categories,
      type,
      display_name
    FROM tmp_dama_migration.sources
;

INSERT INTO data_manager.views (
  view_id,
  source_id,
  data_type,
  interval_version,
  geography_version,
  version,
  source_url,
  publisher,
  table_schema,
  table_name,
  data_table,
  download_url,
  tiles_url,
  start_date,
  end_date,
  last_updated,
  statistics,
  metadata
)
  SELECT
      id,
      source_id,
      data_type,
      interval_version,
      geography_version,
      version,
      source_url,
      publisher,
      FORMAT(
        '%I',
        split_part(
          regexp_replace(
            data_table,
            '[^0-9A-Z_.]',
            '',
            'ig'
          ),
          '.',
          1
        )
      ) AS table_schema,
      FORMAT(
        '%I',
        split_part(
          regexp_replace(
            data_table,
            '[^0-9A-Z_.]',
            '',
            'ig'
          ),
          '.',
          2
        )
      ) AS table_name,
      data_table,
      download_url,
      tiles_url,
      start_date,
      end_date,
      last_updated,
      statistics,
      metadata
    FROM tmp_dama_migration.views
;

COMMIT ;
