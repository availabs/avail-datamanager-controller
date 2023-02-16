CREATE SCHEMA IF NOT EXISTS _data_manager_admin ;
/*
    If a dama's metadata column is null,
      use an existing dama view to initialize it.
*/

-- Tries to replicate Lodash's snakeCase function.
CREATE OR REPLACE FUNCTION _data_manager_admin.to_snake_case( p_string TEXT )
  RETURNS TEXT
  LANGUAGE SQL
  IMMUTABLE
  RETURNS NULL ON NULL INPUT
  AS
  $$
    SELECT
      TRIM(
        BOTH '_'
        FROM LOWER(
          regexp_replace(
            regexp_replace(
              regexp_replace(
                regexp_replace(
                  regexp_replace(
                    p_string,
                    -- non-alphanumeric characters to _
                    '[^0-9A-Z]',
                    '_',
                    'gi'
                  ),
                  -- to snake case: https://stackoverflow.com/a/49521016/3970755
                  '([[:lower:]])([[:upper:]])',
                  '\1_\2',
                  'g'
                ),
                -- alpha preceeds numeric separate with _
                '([a-z])([0-9])',
                '\1_\2',
                'gi'
              ),
              -- numeric preceeds alpha separate with _
              '([0-9])([a-z])',
              '\1_\2',
              'gi'
            ),
            -- multiple _ to single
            '_+',
            '_',
            'g'
          )
        )
      )
  $$
;

CREATE OR REPLACE FUNCTION _data_manager_admin.create_dama_source_global_id(
  damaSourceId INTEGER
)
  RETURNS TEXT
  LANGUAGE SQL
  IMMUTABLE
  RETURNS NULL ON NULL INPUT
  AS
  $$
    SELECT data_manager.dama_db_id() || '_s' || damaSourceId
  $$
;

CREATE OR REPLACE FUNCTION _data_manager_admin.create_dama_view_global_id(
  damaSourceId INTEGER,
  damaViewId INTEGER
)
  RETURNS TEXT
  LANGUAGE SQL
  IMMUTABLE
  RETURNS NULL ON NULL INPUT
  AS
  $$
    SELECT _data_manager_admin.create_dama_source_global_id( damaSourceId ) || '_v' || damaViewId
  $$
;

CREATE OR REPLACE FUNCTION _data_manager_admin.dama_view_global_id( damaViewId INTEGER )
  RETURNS TEXT
  LANGUAGE SQL
  IMMUTABLE
  RETURNS NULL ON NULL INPUT
  AS
  $$
    SELECT _data_manager_admin.create_dama_view_global_id( a.source_id, a.view_id )
      FROM data_manager.views AS a
      WHERE ( view_id = damaViewId )
  $$
;

CREATE OR REPLACE FUNCTION _data_manager_admin.dama_view_name_prefix( damaViewId INTEGER )
  RETURNS TEXT
  LANGUAGE SQL
  IMMUTABLE
  RETURNS NULL ON NULL INPUT
  AS
  $$
    SELECT 
        ( 's' || source_id::TEXT || '_v' || view_id::TEXT )
      FROM data_manager.views AS a
      WHERE ( view_id = damaViewId )
  $$
;

CREATE OR REPLACE FUNCTION _data_manager_admin.dama_view_name(
  damaViewId INTEGER
)
  RETURNS TEXT
  LANGUAGE SQL
  IMMUTABLE
  RETURNS NULL ON NULL INPUT
  AS
  $$
    SELECT
        --  Max Postgres DB object name is 63 characters.
        --    We need to leave some space for index/trigger name extensions
        substring(
          ( dama_view_prefix || '_' || dama_src_normalized_name )
          FROM 1 FOR 50
        )
      FROM (
        SELECT
            _data_manager_admin.dama_view_name_prefix(damaViewId) as dama_view_prefix,
            _data_manager_admin.to_snake_case(a.name) AS dama_src_normalized_name
          FROM data_manager.sources AS a
            INNER JOIN data_manager.views AS b
              USING ( source_id )
          WHERE ( view_id = damaViewId )
      ) AS t
  $$
;
