/*
    If a dama's metadata column is null,
      use an existing dama view to initialize it.
*/


CREATE OR REPLACE VIEW _data_manager_admin.dama_views_missing_tables AS
  SELECT
      a.source_id,
      a.view_id
    FROM data_manager.views AS a
      FULL OUTER JOIN information_schema.tables AS b
        USING (table_schema, table_name)
    WHERE ( b.table_catalog IS NULL )
;

-- CREATE OR REPLACE VIEW _data_manager_admin.dama_views_metadata_conformity AS
--   WITH cte_sources_meta AS (
--     SELECT
--         source_id,
--         COALESCE(metadata, '[]'::JSONB) AS metadata
--       FROM data_manager.sources
--   ), cte_sources_meta_elems AS (
--     SELECT
--         source_id,
--         jsonb_array_elements(
--           metadata
--         ) AS meta_elem
--       FROM cte_sources_meta
--   ), cte_views_meta_elems AS (
--     SELECT
--         source_id,
--         view_id,
--         jsonb_array_elements(table_simplified_schema) as meta_elem
--       FROM _data_manager_admin.dama_table_json_schema
--   ), cte_source_meta_only AS (
--     SELECT
--         b.source_id,
--         b.view_id,
--         a.meta_elem
--       FROM cte_sources_meta_elems AS a
--         INNER JOIN cte_views_meta_elems AS b
--           USING (source_id)
--     EXCEPT
--     SELECT
--         b.source_id,
--         b.view_id,
--         b.meta_elem
--       FROM cte_views_meta_elems AS b
--   ), cte_view_meta_only AS (
--     SELECT
--         b.source_id,
--         b.view_id,
--         b.meta_elem
--       FROM cte_views_meta_elems AS b
--     EXCEPT
--     SELECT
--         b.source_id,
--         b.view_id,
--         a.meta_elem
--       FROM cte_sources_meta_elems AS a
--         INNER JOIN cte_views_meta_elems AS b
--           USING (source_id)
--   ), cte_view_metadata_summary AS (
--     SELECT
--         source_id,
--         view_id,
--         jsonb_agg(DISTINCT b.meta_elem)
--           FILTER (WHERE b.meta_elem IS NOT NULL) AS source_metadata_only,
--         jsonb_agg(DISTINCT c.meta_elem)
--           FILTER (WHERE c.meta_elem IS NOT NULL) AS view_metadata_only
--       FROM _data_manager_admin.dama_table_column_types AS a
--         FULL OUTER JOIN cte_source_meta_only AS b
--           USING (source_id, view_id)
--         FULL OUTER JOIN cte_view_meta_only AS c
--           USING (source_id, view_id)
--       GROUP BY source_id, view_id
--   )
--     SELECT
--         source_id,
--         view_id,
--         source_metadata_only,
--         view_metadata_only,
--         (
--           ( source_metadata_only IS NULL )
--           AND
--           ( view_metadata_only IS NULL )
--         ) AS view_metadata_is_conformant
--       FROM cte_view_metadata_summary
-- ;

CREATE OR REPLACE VIEW _data_manager_admin.dama_source_distinct_view_metadata AS
  SELECT
      source_id,
      COUNT(1) AS distinct_view_metadata_count,
      jsonb_agg( view_metadata_summary ) AS views_metadata_summary
    FROM (
      SELECT
          source_id,
          jsonb_build_object(
            'view_metadata',
            view_metadata,

            'equals_src_metadata',
            bool_and( src_metadata = view_metadata ),

            'view_ids',
            jsonb_agg(view_id ORDER BY view_id)
          ) AS view_metadata_summary
        FROM (
          SELECT
              b.source_id,
              b.view_id,

              ( a.metadata - 'desc' ) AS src_metadata,
              ( b.table_simplified_schema - 'desc' ) AS view_metadata

            FROM data_manager.sources AS a
              INNER JOIN _data_manager_admin.dama_table_json_schema AS b
                USING ( source_id )
        ) AS t
        GROUP BY source_id, view_metadata
    ) AS t
    GROUP BY source_id
;

CREATE OR REPLACE VIEW _data_manager_admin.dama_source_distinct_view_table_schemas AS
  SELECT
      source_id,
      COUNT(1) AS distinct_view_table_schemas,
      jsonb_agg( table_schemas_summary ) AS table_schemas_summary
    FROM (
      SELECT
          source_id,
          jsonb_build_object(
            'table_schema',
            table_schema,

            'view_ids',
            jsonb_agg(view_id ORDER BY view_id)
          ) AS table_schemas_summary
        FROM (
          SELECT
              source_id,
              view_id,

              jsonb_object_agg(
                column_name,
                column_type
              ) AS table_schema

            FROM _data_manager_admin.dama_table_column_types
            GROUP BY source_id, view_id
        ) AS t
        GROUP BY source_id, table_schema
    ) AS t
    GROUP BY source_id
;


CREATE OR REPLACE VIEW _data_manager_admin.dama_views_column_type_variance AS
  SELECT
      source_id,
      column_name,

      COUNT(DISTINCT db_type_instances->>'type') AS distinct_db_types_count,
      COUNT(DISTINCT meta_type_instances->>'type') AS distinct_meta_types_count,

      jsonb_agg(
        DISTINCT db_type_instances ORDER BY db_type_instances
      ) AS db_type_instances,

      jsonb_agg(
        DISTINCT meta_type_instances ORDER BY meta_type_instances
      ) AS meta_type_instances

    FROM (
      SELECT
          source_id,
          metadata->>'name' AS column_name,
          jsonb_build_object(
            'type',
            metadata->>'type',

            'view_ids',
            jsonb_agg(view_id ORDER BY 1)
          ) AS meta_type_instances
        FROM (
          SELECT
              source_id,
              view_id,
              jsonb_array_elements(table_simplified_schema) AS metadata
            FROM _data_manager_admin.dama_table_json_schema
        ) AS t
        GROUP BY source_id, metadata->>'name', metadata->>'type'
    ) AS x INNER JOIN (
      SELECT
          source_id,
          column_name,
          jsonb_build_object(
            'type',
            column_type,

            'view_ids',
            jsonb_agg(view_id ORDER BY 1)
          ) AS db_type_instances
        FROM _data_manager_admin.dama_table_column_types
        GROUP BY source_id, column_name, column_type
    ) AS y USING ( source_id, column_name )
    GROUP BY 1,2
;

CREATE OR REPLACE PROCEDURE _data_manager_admin.initialize_dama_src_metadata_using_view (
    p_view_id INTEGER
  )
    LANGUAGE plpgsql
    AS $$

      DECLARE
        v_source_id         INTEGER;
        v_source_has_meta   BOOLEAN;

      BEGIN

        EXECUTE(
          FORMAT('
            SELECT
                source_id
              FROM data_manager.views
              WHERE ( view_id = %L )
            ',
            p_view_id
          )
        ) INTO v_source_id ;

        IF (v_source_id IS NULL) THEN
          RAISE EXCEPTION 'Invalid DataManager View ID: %', p_view_id ;
        END IF ;

        EXECUTE(
          FORMAT('
            SELECT
                metadata IS NOT NULL
              FROM data_manager.sources
              WHERE ( source_id = %L )
            ',
            v_source_id
          )
        ) INTO v_source_has_meta ;

        IF (v_source_has_meta) THEN
          RAISE EXCEPTION 'DataManager Source metadata MUST be null to initialize.' ;
        END IF;

        UPDATE data_manager.sources AS a
          SET metadata = b.metadata
          FROM (
            SELECT
                source_id,
                table_simplified_schema AS metadata
              FROM _data_manager_admin.dama_table_json_schema
              WHERE ( view_id = p_view_id )
          ) AS b
          WHERE (
            ( a.source_id = b.source_id )
            AND
            ( a.metadata IS NULL )
          )
        ;

    END ;

  $$
;

-- Query based on https://stackoverflow.com/a/9985338/3970755
CREATE OR REPLACE VIEW _data_manager_admin.dama_views_int_ids
  AS
    SELECT
        source_id,
        view_id,
        table_schema,
        table_name,
        primary_key_summary,
        has_simple_int_pkey,
        has_dama_id_col,

        CASE
          WHEN has_dama_id_col
            THEN '__id__'
          WHEN has_simple_int_pkey
            THEN primary_key_summary->0->>'column_name'
          ELSE NULL
        END AS int_id_column_name
      FROM (

        SELECT
            t.source_id,
            t.view_id,
            t.table_schema,
            t.table_name,
            t.primary_key_summary,

            (
              ( jsonb_array_length(t.primary_key_summary) = 1 )
              AND
              ( t.primary_key_summary->0->>'column_type' IN ( 'bigint', 'integer', 'smallint' ) )
            ) AS has_simple_int_pkey,

            ( c.column_name IS NOT NULL ) AS has_dama_id_col

          FROM (
            SELECT
                v.source_id,
                v.view_id,
                v.table_schema,
                v.table_name,

                jsonb_agg(
                  jsonb_build_object(
                    'column_name',
                    c.column_name,

                    'column_type',
                    c.data_type
                  )
                ) AS primary_key_summary

              FROM data_manager.views AS v
                INNER JOIN information_schema.table_constraints a
                  USING ( table_schema, table_name )
                INNER JOIN information_schema.constraint_column_usage AS b
                  USING (constraint_schema, constraint_name)
                INNER JOIN information_schema.columns AS c
                  ON (
                    ( c.table_schema = a.constraint_schema )
                    AND
                    ( a.table_name = c.table_name )
                    AND
                    ( b.column_name = c.column_name )
                  )
              WHERE ( constraint_type = 'PRIMARY KEY' )
              GROUP BY 1,2,3,4
        ) AS t
          LEFT OUTER JOIN information_schema.columns AS c
            ON (
              -- FIXME: Make there there is a UNIQUE constraint on the column
              ( t.table_schema = c.table_schema )
              AND
              ( t.table_name = c.table_name )
              AND
              ( c.column_name = '__id__' )
              AND
              ( c.is_nullable = 'NO' )
              AND
              ( column_default LIKE 'nextval(%' )
            )

      ) AS t
;

DROP VIEW IF EXISTS _data_manager_admin.dama_sources_comprehensive CASCADE;

CREATE OR REPLACE VIEW _data_manager_admin.dama_sources_comprehensive
  AS
    SELECT
        *,
        _data_manager_admin.create_dama_source_global_id(source_id) AS dama_global_id,
        _data_manager_admin.to_snake_case(name) AS dama_src_normalized_name
      FROM data_manager.sources
        NATURAL LEFT JOIN _data_manager_admin.dama_source_distinct_view_table_schemas
;

DROP VIEW IF EXISTS _data_manager_admin.dama_views_comprehensive CASCADE;

CREATE OR REPLACE VIEW _data_manager_admin.dama_views_comprehensive
  AS
    SELECT
        *,
        _data_manager_admin.dama_view_global_id(view_id) AS dama_global_id,
        _data_manager_admin.dama_view_name_prefix(view_id) AS dama_view_name_prefix,
        _data_manager_admin.dama_view_name(view_id) AS dama_view_name
      FROM data_manager.views
        NATURAL LEFT JOIN _data_manager_admin.dama_views_int_ids
        --
        -- below joins are never used anywhere in server code and currently
        -- break the view ERROR:  cannot extract elements from an object
        --
        --NATURAL LEFT JOIN _data_manager_admin.dama_views_missing_tables
        --NATURAL LEFT JOIN _data_manager_admin.dama_views_metadata_conformity
        LEFT OUTER JOIN (
          SELECT
              view_id,
              geojson_type
            FROM (
              SELECT
                  view_id,
                  json_type->'properties'->'type'->'enum'->>0 as geojson_type,
                  row_number() OVER (PARTITION BY view_id ORDER BY column_number) AS row_number
                FROM _data_manager_admin.dama_table_column_types
                WHERE ( is_geometry_col )
            ) AS t
            WHERE ( row_number = 1 )
        ) AS x USING ( view_id )
;
