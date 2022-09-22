-- PostgreSQL database dump
--

-- Dumped from database version 11.5 (Ubuntu 11.5-3.pgdg18.04+1)
-- Dumped by pg_dump version 11.17 (Ubuntu 11.17-1.pgdg18.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: _data_manager_admin; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA _data_manager_admin;


--
-- Name: data_manager; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA data_manager;


--
-- Name: etl_health_accumulator_fn(); Type: FUNCTION; Schema: _data_manager_admin; Owner: -
--

CREATE FUNCTION _data_manager_admin.etl_health_accumulator_fn() RETURNS TABLE(data_manager_view_id integer, expected_update_interval interval, actual_update_interval interval, etl_health_status text)
    LANGUAGE plpgsql
    AS $$

    DECLARE

      admin_schema TEXT     ;
      admin_schemas TEXT[]  ;
      query_clauses TEXT[]  ;

    BEGIN

        SELECT
            ARRAY_AGG(table_schema)
          FROM (
            SELECT
                table_schema,
                table_name,
                array_agg(column_name::TEXT) column_names
              FROM _data_manager_admin.table_column_types
              WHERE (
                ( table_schema != '_data_manager_admin' ) -- Else infinite loop
                AND
                ( table_schema LIKE '%_admin' )
                AND
                ( table_name = 'etl_health' )
                AND
                (
                  (
                    ( column_name = 'data_manager_view_id' )
                    AND
                    ( column_type = 'integer' )
                  )
                  OR
                  (
                    ( column_name = 'expected_update_interval' )
                    AND
                    ( column_type = 'interval' )
                  )
                  OR
                  (
                    ( column_name = 'actual_update_interval' )
                    AND
                    ( column_type = 'interval' )
                  )
                  OR
                  (
                    ( column_name = 'etl_health_status' )
                    AND
                    ( column_type = 'text' )
                  )
                )
              )
              GROUP BY 1,2
          ) AS a
          WHERE ( column_names @> ARRAY[
              'data_manager_view_id',
              'expected_update_interval',
              'actual_update_interval',
              'etl_health_status'
            ]
          )
      INTO admin_schemas ;

      FOREACH admin_schema IN ARRAY admin_schemas
        LOOP
          query_clauses := query_clauses || FORMAT('
            SELECT
                data_manager_view_id,
                expected_update_interval,
                actual_update_interval,
                etl_health_status
              FROM %I.etl_health',
            admin_schema
          ) ;
        END LOOP
      ;

      RETURN QUERY EXECUTE array_to_string(query_clauses, '
          UNION ALL'
      ) ;

    END ;
  $$;


--
-- Name: initialize_sources_metadata(text); Type: PROCEDURE; Schema: _data_manager_admin; Owner: -
--

CREATE PROCEDURE _data_manager_admin.initialize_sources_metadata(schemaname_param text)
    LANGUAGE sql
    AS $$

      WITH cte_src_meta AS (
        SELECT
            b.source_id AS id,
            a.table_simplified_schema AS metadata
          FROM _data_manager_admin.table_json_schema AS a
            INNER JOIN (
              SELECT
                  source_id,
                  regexp_split_to_array(
                    regexp_replace(
                      max(data_table)::TEXT,
                      '"',
                      '', 
                      'g'
                    ),
                    '\.'
                  ) AS table_name_arr
                FROM data_manager.views
                GROUP BY 1
            ) AS b
              ON (
                ( a.table_schema = b.table_name_arr[1] )
                AND
                ( a.table_schema = schemaname_param )
                AND
                ( a.table_name = b.table_name_arr[2] )
              )
      )
        UPDATE data_manager.sources AS a
          SET metadata = b.metadata
          FROM cte_src_meta AS b
          WHERE (
            ( a.id = b.id )
            AND
            ( a.metadata IS NULL )
          )
      ;

  $$;


--
-- Name: table_schema_as_json_schema(text, text); Type: FUNCTION; Schema: _data_manager_admin; Owner: -
--

CREATE FUNCTION _data_manager_admin.table_schema_as_json_schema(schemaname text, tablename text) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$                        
    DECLARE
      json_schema   JSONB ;

    BEGIN

    EXECUTE FORMAT('
      SELECT
          jsonb_build_object(
            ''type'',         ''object'',
            ''properties'',   jsonb_object_agg(
                                property_name,
                                property_type
                              ),
            ''required'',     json_agg(
                                property_name ORDER BY property_name
                              ) FILTER (WHERE is_required)
          ) AS table_schema
        FROM (
          SELECT
              column_name AS property_name,
              CASE
                WHEN (
                  data_type = ''boolean''
                ) THEN 
                    jsonb_build_object(
                      ''type'',   ''boolean''
                    )

                WHEN (
                  data_type IN (
                    ''"char"'', 
                    ''character'', 
                    ''character varying'', 
                    ''text''
                  )
                ) THEN 
                    jsonb_build_object(
                      ''type'',   ''string''
                    )

                WHEN (
                  data_type IN (
                    ''bigint'', 
                    ''integer'', 
                    ''smallint''
                  )
                ) THEN
                    jsonb_build_object(
                      ''type'',           ''integer'',
                      ''description'',    data_type
                    )

                WHEN (
                  data_type IN (
                    ''double precision'', 
                    ''numeric'', 
                    ''real''
                  )
                ) THEN
                    jsonb_build_object(
                      ''type'',           ''number'',
                      ''description'',    data_type
                    )

                -- NOTE: This is not necessarily true. "foo" is a valid JSON value.
                WHEN (
                  data_type IN (
                    ''json'', 
                    ''jsonb''
                  )
                ) THEN
                    jsonb_build_object(
                      ''type'',   ''object''
                    )

                WHEN (
                  data_type IN (
                    ''date'',
                    ''timestamp without time zone'',
                    ''timestamp with time zone''
                  )
                ) THEN
                    jsonb_build_object(
                      ''type'',           ''string'',
                      ''format'',         ''date-time'',
                      ''description'',    data_type
                    )

                WHEN (
                  data_type = ''ARRAY''
                ) THEN
                    jsonb_build_object(
                      ''type'',     ''array'',
                      ''items'',    CASE
                                      WHEN ( starts_with(udt_name , ''_float'') )
                                        THEN
                                          jsonb_build_object(
                                            ''type'',   ''number''
                                          )
                                      WHEN ( starts_with(udt_name, ''_int'') )
                                        THEN
                                          jsonb_build_object(
                                            ''type'',   ''integer''
                                          )
                                      WHEN ( udt_name IN (''_text'', ''_varchar'' ) )
                                        THEN
                                          jsonb_build_object(
                                            ''type'',   ''string''
                                          )
                                      ELSE 
                                        jsonb_build_object(
                                          ''description'',  ''user-defined data type'',
                                          ''type'',         ''string''
                                        )
                                    END
                  )

              ELSE jsonb_build_object(
                      ''description'',  ''user-defined data type'',
                      ''type'',         ''string''
                    )

            END AS property_type,

            ( is_nullable = ''NO'' ) AS is_required
          FROM information_schema.columns
          WHERE (
            ( table_schema = %L )
            AND
            ( table_name = %L )
          )
        ) AS t
      ',
      schemaname,
      tablename
    ) INTO json_schema ;

    RETURN json_schema ;
  END ;

  $$;


--
-- Name: etl_health_statuses; Type: VIEW; Schema: _data_manager_admin; Owner: -
--

CREATE VIEW _data_manager_admin.etl_health_statuses AS
 SELECT etl_health_accumulator_fn.data_manager_view_id,
    etl_health_accumulator_fn.expected_update_interval,
    etl_health_accumulator_fn.actual_update_interval,
    etl_health_accumulator_fn.etl_health_status
   FROM _data_manager_admin.etl_health_accumulator_fn() etl_health_accumulator_fn(data_manager_view_id, expected_update_interval, actual_update_interval, etl_health_status);


SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: geojson_json_schemas; Type: TABLE; Schema: _data_manager_admin; Owner: -
--

CREATE TABLE _data_manager_admin.geojson_json_schemas (
    geojson_type text NOT NULL,
    json_schema jsonb NOT NULL,
    CONSTRAINT geojson_type_check CHECK ((geojson_type = ANY (ARRAY['FeatureCollection'::text, 'Feature'::text, 'Geometry'::text, 'GeometryCollection'::text, 'MultiPolygon'::text, 'MultiLineString'::text, 'MultiPoint'::text, 'Polygon'::text, 'LineString'::text, 'Point'::text])))
)
WITH (fillfactor='100');


--
-- Name: table_column_types; Type: VIEW; Schema: _data_manager_admin; Owner: -
--

CREATE VIEW _data_manager_admin.table_column_types AS
 SELECT ((a.relnamespace)::regnamespace)::text AS table_schema,
    (a.relname)::text AS table_name,
    b.attname AS column_name,
    format_type(b.atttypid, b.atttypmod) AS column_type,
    b.attnotnull AS column_not_null,
    b.attnum AS column_number
   FROM (pg_class a
     JOIN pg_attribute b ON ((a.oid = b.attrelid)))
  WHERE ((NOT b.attisdropped) AND (b.attnum > 0));


--
-- Name: table_column_types_with_json_types; Type: VIEW; Schema: _data_manager_admin; Owner: -
--

CREATE VIEW _data_manager_admin.table_column_types_with_json_types AS
 SELECT t.table_schema,
    t.table_name,
    t.column_name,
    t.column_type,
    t.column_not_null,
    t.column_number,
        CASE
            WHEN t.is_array THEN jsonb_build_object('type', 'array', 'items', t.item_type, '$comment', t.column_type)
            ELSE t.item_type
        END AS json_type
   FROM ( SELECT a.table_schema,
            a.table_name,
            a.column_name,
            a.column_type,
            a.column_not_null,
            a.column_number,
                CASE
                    WHEN (b.json_schema IS NOT NULL) THEN b.json_schema
                    WHEN (a.column_type = 'boolean'::text) THEN jsonb_build_object('type', 'boolean')
                    WHEN starts_with(a.column_type, '"char"'::text) THEN jsonb_build_object('type', 'string', 'minLength', 1, 'maxLength', 1)
                    WHEN starts_with(a.column_type, 'character('::text) THEN jsonb_build_object('type', 'string', 'minLength', ("substring"(a.column_type, '\d+'::text))::integer, 'maxLength', ("substring"(a.column_type, '\d+'::text))::integer, '$comment', regexp_replace(a.column_type, '\[\]$'::text, ''::text))
                    WHEN starts_with(a.column_type, 'character varying('::text) THEN jsonb_build_object('type', 'string', 'minLength', 0, 'maxLength', ("substring"(a.column_type, '\d+'::text))::integer, '$comment', regexp_replace(a.column_type, '\[\]$'::text, ''::text))
                    WHEN (starts_with(a.column_type, 'character varying'::text) OR starts_with(a.column_type, 'text'::text)) THEN jsonb_build_object('type', 'string', 'minLength', 0, '$comment', regexp_replace(a.column_type, '\[\]$'::text, ''::text))
                    WHEN starts_with(a.column_type, 'smallint'::text) THEN jsonb_build_object('type', 'integer', 'minimum', '-32768'::integer, 'maximum', 32767, '$comment', regexp_replace(a.column_type, '\[\]$'::text, ''::text))
                    WHEN starts_with(a.column_type, 'integer'::text) THEN jsonb_build_object('type', 'integer', 'minimum', '-2147483648'::integer, 'maximum', 2147483647, '$comment', regexp_replace(a.column_type, '\[\]$'::text, ''::text))
                    WHEN starts_with(a.column_type, 'bigint'::text) THEN jsonb_build_object('type', 'integer', 'minimum', '-9223372036854775808'::bigint, 'maximum', '9223372036854775807'::bigint, '$comment', regexp_replace(a.column_type, '\[\]$'::text, ''::text))
                    WHEN starts_with(a.column_type, 'smallserial'::text) THEN jsonb_build_object('type', 'integer', 'minimum', 1, 'maximum', 32767, '$comment', regexp_replace(a.column_type, '\[\]$'::text, ''::text))
                    WHEN starts_with(a.column_type, 'serial'::text) THEN jsonb_build_object('type', 'integer', 'minimum', 1, 'maximum', 2147483647, '$comment', regexp_replace(a.column_type, '\[\]$'::text, ''::text))
                    WHEN starts_with(a.column_type, 'bigserial'::text) THEN jsonb_build_object('type', 'integer', 'minimum', 1, 'maximum', '9223372036854775807'::bigint, '$comment', regexp_replace(a.column_type, '\[\]$'::text, ''::text))
                    WHEN (starts_with(a.column_type, 'double precision'::text) OR starts_with(a.column_type, 'numeric'::text) OR starts_with(a.column_type, 'real'::text)) THEN jsonb_build_object('type', 'number', '$comment', regexp_replace(a.column_type, '\[\]$'::text, ''::text))
                    WHEN starts_with(a.column_type, 'json'::text) THEN jsonb_build_object('type', json_build_array('string', 'number', 'integer', 'object', 'array', 'boolean', 'null'), '$comment', regexp_replace(a.column_type, '\[\]$'::text, ''::text))
                    WHEN (a.column_type = ANY (ARRAY['date'::text, 'timestamp without time zone'::text, 'timestamp with time zone'::text])) THEN jsonb_build_object('type', 'string', 'format', 'date-time', 'description', a.column_type)
                    ELSE jsonb_build_object('type', json_build_array('string', 'number', 'integer', 'object', 'array', 'boolean', 'null'), '$comment', regexp_replace(a.column_type, '\[\]$'::text, ''::text))
                END AS item_type,
            (a.column_type ~~ '%[]'::text) AS is_array
           FROM (_data_manager_admin.table_column_types a
             LEFT JOIN _data_manager_admin.geojson_json_schemas b ON ((a.column_type ~ (('.*geometry\('::text || b.geojson_type) || '.*\)$'::text))))) t;


--
-- Name: table_json_schema; Type: VIEW; Schema: _data_manager_admin; Owner: -
--

CREATE VIEW _data_manager_admin.table_json_schema AS
 SELECT table_column_types_with_json_types.table_schema,
    table_column_types_with_json_types.table_name,
    jsonb_build_object('$schema', 'http://json-schema.org/draft-07/schema#', 'type', 'object', 'properties', jsonb_object_agg(table_column_types_with_json_types.column_name, table_column_types_with_json_types.json_type), 'required', json_agg(table_column_types_with_json_types.column_name ORDER BY table_column_types_with_json_types.column_name) FILTER (WHERE table_column_types_with_json_types.column_not_null)) AS table_json_schema,
    jsonb_agg(jsonb_build_object('name', table_column_types_with_json_types.column_name, 'type',
        CASE
            WHEN (jsonb_typeof((table_column_types_with_json_types.json_type -> 'type'::text)) = 'array'::text) THEN '"object"'::jsonb
            ELSE (table_column_types_with_json_types.json_type -> 'type'::text)
        END, 'desc', NULL::unknown) ORDER BY table_column_types_with_json_types.column_number) AS table_simplified_schema
   FROM _data_manager_admin.table_column_types_with_json_types
  GROUP BY table_column_types_with_json_types.table_schema, table_column_types_with_json_types.table_name;


--
-- Name: views; Type: TABLE; Schema: data_manager; Owner: -
--

CREATE TABLE data_manager.views (
    id integer NOT NULL,
    source_id integer NOT NULL,
    data_type text,
    interval_version text,
    geography_version text,
    version text,
    source_url text,
    publisher text,
    data_table text,
    download_url text,
    tiles_url text,
    start_date date,
    end_date date,
    last_updated timestamp without time zone,
    statistics jsonb,
    metadata jsonb
);


--
-- Name: views_id_seq; Type: SEQUENCE; Schema: data_manager; Owner: -
--

CREATE SEQUENCE data_manager.views_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: views_id_seq; Type: SEQUENCE OWNED BY; Schema: data_manager; Owner: -
--

ALTER SEQUENCE data_manager.views_id_seq OWNED BY data_manager.views.id;


--
-- Name: views_copy; Type: TABLE; Schema: _data_manager_admin; Owner: -
--

CREATE TABLE _data_manager_admin.views_copy (
    id integer DEFAULT nextval('data_manager.views_id_seq'::regclass) NOT NULL,
    source_id integer NOT NULL,
    data_type text,
    interval_version text,
    geography_version text,
    version text,
    source_url text,
    publisher text,
    data_table text,
    download_url text,
    tiles_url text,
    start_date date,
    end_date date,
    last_updated timestamp without time zone,
    statistics jsonb,
    metadata jsonb
);


--
-- Name: sources; Type: TABLE; Schema: data_manager; Owner: -
--

CREATE TABLE data_manager.sources (
    id integer NOT NULL,
    name text NOT NULL,
    update_interval text,
    category text[],
    description text,
    statistics jsonb,
    metadata jsonb,
    categories jsonb DEFAULT '[]'::jsonb,
    type character varying,
    display_name character varying
);


--
-- Name: sources_id_seq; Type: SEQUENCE; Schema: data_manager; Owner: -
--

CREATE SEQUENCE data_manager.sources_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: sources_id_seq; Type: SEQUENCE OWNED BY; Schema: data_manager; Owner: -
--

ALTER SEQUENCE data_manager.sources_id_seq OWNED BY data_manager.sources.id;


--
-- Name: sources id; Type: DEFAULT; Schema: data_manager; Owner: -
--

ALTER TABLE ONLY data_manager.sources ALTER COLUMN id SET DEFAULT nextval('data_manager.sources_id_seq'::regclass);


--
-- Name: views id; Type: DEFAULT; Schema: data_manager; Owner: -
--

ALTER TABLE ONLY data_manager.views ALTER COLUMN id SET DEFAULT nextval('data_manager.views_id_seq'::regclass);


--
-- Name: geojson_json_schemas geojson_json_schemas_pkey; Type: CONSTRAINT; Schema: _data_manager_admin; Owner: -
--

ALTER TABLE ONLY _data_manager_admin.geojson_json_schemas
    ADD CONSTRAINT geojson_json_schemas_pkey PRIMARY KEY (geojson_type);


--
-- Name: views_copy views_copy_data_table_key; Type: CONSTRAINT; Schema: _data_manager_admin; Owner: -
--

ALTER TABLE ONLY _data_manager_admin.views_copy
    ADD CONSTRAINT views_copy_data_table_key UNIQUE (data_table);


--
-- Name: views_copy views_copy_pkey; Type: CONSTRAINT; Schema: _data_manager_admin; Owner: -
--

ALTER TABLE ONLY _data_manager_admin.views_copy
    ADD CONSTRAINT views_copy_pkey PRIMARY KEY (id);


--
-- Name: sources sources_name_key; Type: CONSTRAINT; Schema: data_manager; Owner: -
--

ALTER TABLE ONLY data_manager.sources
    ADD CONSTRAINT sources_name_key UNIQUE (name);


--
-- Name: sources sources_pkey; Type: CONSTRAINT; Schema: data_manager; Owner: -
--

ALTER TABLE ONLY data_manager.sources
    ADD CONSTRAINT sources_pkey PRIMARY KEY (id);

ALTER TABLE data_manager.sources CLUSTER ON sources_pkey;


--
-- Name: views views_pkey; Type: CONSTRAINT; Schema: data_manager; Owner: -
--

ALTER TABLE ONLY data_manager.views
    ADD CONSTRAINT views_pkey PRIMARY KEY (id);


--
-- Name: views views_source_data_table_uniq; Type: CONSTRAINT; Schema: data_manager; Owner: -
--

ALTER TABLE ONLY data_manager.views
    ADD CONSTRAINT views_source_data_table_uniq UNIQUE (data_table);


--
-- Name: views views_source_id_fkey; Type: FK CONSTRAINT; Schema: data_manager; Owner: -
--

ALTER TABLE ONLY data_manager.views
    ADD CONSTRAINT views_source_id_fkey FOREIGN KEY (source_id) REFERENCES data_manager.sources(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

