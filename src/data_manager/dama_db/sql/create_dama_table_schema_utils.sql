/*
  The point of these VIEWs is to get detailed descriptions of table schemas.
  We use JSON Schema to support API Servers and Browsers.

  See:
    * https://www.postgresql.org/docs/11/catalog-pg-attribute.html
    * https://gis.stackexchange.com/a/94083
*/

CREATE SCHEMA IF NOT EXISTS _data_manager_admin ;

DROP VIEW IF EXISTS _data_manager_admin.table_column_types CASCADE ;
CREATE OR REPLACE VIEW _data_manager_admin.table_column_types
  AS
    SELECT
        table_schema,
        table_name,

        column_name,
        column_type,
        column_not_null,
        column_number,
        column_pkey_number,

        CASE
          WHEN ( is_array )
            THEN
              jsonb_build_object(
                'type',       'array',
                'items',      item_type,
                '$comment',   column_type
              )
            ELSE item_type
        END AS json_type,

        is_geometry_col

      FROM (
        SELECT
            a.table_schema,
            a.table_name,
            a.column_name,
            a.column_type,
            a.column_not_null,
            a.column_number,
            a.column_pkey_number,

            ( b.json_schema IS NOT NULL ) AS is_geometry_col,

            CASE

              WHEN ( b.json_schema IS NOT NULL )
                THEN b.json_schema

              WHEN (
                a.column_type = 'boolean'
              ) THEN
                  jsonb_build_object(
                    'type',   'boolean'
                  )

            -- Character Types (See https://www.postgresql.org/docs/11/datatype-character.html)
              WHEN (
                starts_with(a.column_type, '"char"')
              ) THEN
                  jsonb_build_object(
                    'type',       'string',
                    'minLength',  1,
                    'maxLength',  1
                  )

              WHEN (
                starts_with(a.column_type, 'character(')
              ) THEN
                  jsonb_build_object(
                    'type',       'string',
                    'minLength',  substring(a.column_type, '\d+')::INTEGER,
                    'maxLength',  substring(a.column_type, '\d+')::INTEGER,
                    '$comment',   regexp_replace(a.column_type, '\[\]$', '')
                  )

              -- variable-length with limit
              WHEN (
                starts_with(a.column_type, 'character varying(')
              ) THEN
                  jsonb_build_object(
                    'type',       'string',
                    'minLength',  0,
                    'maxLength',  substring(a.column_type, '\d+')::INTEGER,
                    '$comment',   regexp_replace(a.column_type, '\[\]$', '')
                  )

              WHEN (
                ( starts_with(a.column_type, 'character varying') )
                OR
                ( starts_with(a.column_type, 'text' ) )
              ) THEN
                  jsonb_build_object(
                    'type',       'string',
                    'minLength',  0,
                    '$comment',   regexp_replace(a.column_type, '\[\]$', '')
                  )

            -- Numeric Types: See https://www.postgresql.org/docs/11/datatype-numeric.html
              WHEN (
                starts_with(a.column_type, 'smallint')
              ) THEN
                  jsonb_build_object(
                    'type',       'integer',
                    'minimum',    -32768,
                    'maximum',    32767,
                    '$comment',   regexp_replace(a.column_type, '\[\]$', '')
                  )

              WHEN (
                starts_with(a.column_type, 'integer')
              ) THEN
                  jsonb_build_object(
                    'type',       'integer',
                    'minimum',    -2147483648,
                    'maximum',    2147483647,
                    '$comment',   regexp_replace(a.column_type, '\[\]$', '')
                  )

              WHEN (
                starts_with(a.column_type, 'bigint')
              ) THEN
                  jsonb_build_object(
                    'type',       'integer',
                    'minimum',    -9223372036854775808,
                    'maximum',    9223372036854775807,
                    '$comment',   regexp_replace(a.column_type, '\[\]$', '')
                  )

              WHEN (
                starts_with(a.column_type, 'smallserial')
              ) THEN
                  jsonb_build_object(
                    'type',       'integer',
                    'minimum',    1,
                    'maximum',    32767,
                    '$comment',   regexp_replace(a.column_type, '\[\]$', '')
                  )

              WHEN (
                starts_with(a.column_type, 'serial')
              ) THEN
                  jsonb_build_object(
                    'type',       'integer',
                    'minimum',    1,
                    'maximum',    2147483647,
                    '$comment',   regexp_replace(a.column_type, '\[\]$', '')
                  )

              WHEN (
                starts_with(a.column_type, 'bigserial')
              ) THEN
                  jsonb_build_object(
                    'type',       'integer',
                    'minimum',    1,
                    'maximum',    9223372036854775807,
                    '$comment',   regexp_replace(a.column_type, '\[\]$', '')
                  )

              WHEN (
                ( starts_with(a.column_type, 'double precision') )
                OR
                ( starts_with(a.column_type, 'numeric') )
                OR
                ( starts_with(a.column_type, 'real') )
              ) THEN
                  jsonb_build_object(
                    'type',        'number',
                    '$comment',    regexp_replace(a.column_type, '\[\]$', '')
                  )

              WHEN (
                starts_with(a.column_type, 'json')
              ) THEN
                  jsonb_build_object(
                    'type',       json_build_array(
                                    'string',
                                    'number',
                                    'integer',
                                    'object',
                                    'array',
                                    'boolean',
                                    'null'
                                  ),
                    '$comment',   regexp_replace(a.column_type, '\[\]$', '')
                  )

              WHEN (
                a.column_type IN (
                  'date',
                  'timestamp without time zone',
                  'timestamp with time zone'
                )
              ) THEN
                  jsonb_build_object(
                    'type',           'string',
                    'format',         'date-time',
                    'description',    a.column_type
                  )

              ELSE
                  jsonb_build_object(
                    'type',       json_build_array(
                                    'string',
                                    'number',
                                    'integer',
                                    'object',
                                    'array',
                                    'boolean',
                                    'null'
                                  ),
                    '$comment',   regexp_replace(a.column_type, '\[\]$', '')
                  )
            END AS item_type,

            ( a.column_type LIKE '%[]' ) AS is_array

          FROM (
            SELECT
                a.relnamespace::regnamespace::TEXT AS table_schema,
                a.relname::TEXT AS table_name,
                b.attname AS column_name,
                format_type(b.atttypid, b.atttypmod) AS column_type,
                b.attnotnull AS column_not_null,
                b.attnum AS column_number,
                (
                  array_position(
                    string_to_array(d.indkey::text, ' ')::int2[],
                    b.attnum
                  )
                ) AS column_pkey_number
              FROM pg_catalog.pg_class AS a
                INNER JOIN pg_catalog.pg_type AS x
                  ON (
                    ( a.reltype = x.oid )
                    -- -- FIXME FIXME FIXME: Need to filter out indexes and other no-data objects.
                    -- AND
                    -- ( x.typname = ANY(ARRAY['tables', 'views', 'sources']) )
                  )
                INNER JOIN pg_catalog.pg_attribute AS b
                  ON (a.oid = b.attrelid)
                INNER JOIN (
                  SELECT
                      a.table_schema,
                      a.table_name
                    FROM data_manager.views AS a
                      -- make sure the tables actually exist so ::regclass::oid below does not raise exception
                      INNER JOIN pg_catalog.pg_tables AS b
                        ON (
                          ( a.table_schema = b.schemaname )
                          AND
                          ( a.table_name = b.tablename )
                        )
                  UNION ALL
                  SELECT
                      schemaname AS table_schema,
                      tablename AS table_name
                    FROM pg_tables
                    WHERE (
                      ( schemaname = 'data_manager' )
                      OR
                      ( schemaname = '_data_manager_admin' )
                    )

                ) AS c
                  ON ( a.oid = format('%I.%I', c.table_schema, c.table_name)::regclass::oid )
                LEFT OUTER JOIN pg_catalog.pg_index AS d
                  ON (
                    ( d.indisprimary )
                    AND
                    ( a.oid = d.indrelid )
                    AND
                    ( b.attnum = ANY(d.indkey) )
                  )
              WHERE (
                ( NOT b.attisdropped )
                AND
                ( b.attnum > 0 )
              )
          ) AS a
            LEFT OUTER JOIN _data_manager_admin.geojson_json_schemas AS b
              ON (
                ( a.column_type ~ ('.*geometry\(' || b.geojson_type || '.*\)$') )
              )
      ) AS t
;

--  DROP VIEW IF EXISTS _data_manager_admin.table_json_schema ;
CREATE OR REPLACE VIEW _data_manager_admin.table_json_schema
  AS
    SELECT
        table_schema,
        table_name,

        jsonb_build_object(
          '$schema',      'http://json-schema.org/draft-07/schema#',
          'type',         'object',
          'properties',   jsonb_object_agg(
                            column_name,
                            json_type
                          ),
          'required',     json_agg(
                            column_name ORDER BY column_name
                          ) FILTER (WHERE column_not_null)
          ) AS table_json_schema,

        jsonb_agg(
          jsonb_build_object(
            'name',     column_name,
            'type',     CASE
                          WHEN ( jsonb_typeof(json_type->'type') = 'array' )
                            THEN '"object"'::JSONB
                          ELSE json_type->'type'
                        END,
            'desc',     null
          )
          ORDER BY column_number
        ) AS table_simplified_schema

      FROM _data_manager_admin.table_column_types
      GROUP BY table_schema, table_name
;

CREATE OR REPLACE VIEW _data_manager_admin.dama_table_column_types
  AS
    SELECT
        a.source_id,
        a.view_id,
        b.*
      FROM data_manager.views AS a
        INNER JOIN _data_manager_admin.table_column_types AS b
          USING (table_schema, table_name)
;

CREATE OR REPLACE VIEW _data_manager_admin.dama_table_json_schema
  AS
    SELECT
        a.source_id,
        a.view_id,
        b.*
      FROM data_manager.views AS a
        INNER JOIN _data_manager_admin.table_json_schema AS b
          USING (table_schema, table_name)
;
