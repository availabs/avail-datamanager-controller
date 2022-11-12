/*
    If a dama's metadata column is null,
      use an existing dama view to initialize it.
*/

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
        --    We need to leave some space (10 characters) for index/trigger name extensions
        substring(
          dama_src_normalized_name
          FROM 1 FOR (63 - 10 - char_length(dama_view_suffix))
        ) || dama_view_suffix
      FROM (
        SELECT
            _data_manager_admin.to_snake_case(a.name) AS dama_src_normalized_name,
            ( '_s' || source_id::TEXT || '_v' || view_id::TEXT ) AS dama_view_suffix
          FROM data_manager.sources AS a
            INNER JOIN data_manager.views AS b
              USING ( source_id )
          WHERE ( view_id = damaViewId )
      ) AS t
  $$
;
