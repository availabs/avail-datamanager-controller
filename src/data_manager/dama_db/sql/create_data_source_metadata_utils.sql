/*
    If a dama's metadata column is null,
      use an existing dama view to initialize it.
*/


CREATE OR REPLACE VIEW _data_manager_admin.dama_views_missing_tables AS
  SELECT
      a.source_id,
      a.id AS view_id
    FROM data_manager.views AS a
      FULL OUTER JOIN information_schema.tables AS b
        USING (table_schema, table_name)
    WHERE ( b.table_catalog IS NULL )
;

CREATE OR REPLACE VIEW _data_manager_admin.dama_views_metadata_compliance AS
  WITH cte_sources_meta AS (
    SELECT
        id AS source_id,
        COALESCE(metadata, '[]'::JSONB) AS metadata
      FROM data_manager.sources
  ), cte_views_meta AS (
    SELECT
        a.source_id,
        a.id AS view_id,
        b.table_simplified_schema AS metadata
      FROM data_manager.views AS a
        INNER JOIN _data_manager_admin.table_json_schema AS b
          USING ( table_schema, table_name )
  ), cte_sources_meta_elems AS (
    SELECT
        source_id,
        jsonb_array_elements(
          metadata
        ) AS meta_elem
      FROM cte_sources_meta
  ), cte_views_meta_elems AS (
    SELECT
        source_id,
        view_id,
        jsonb_array_elements(metadata) as meta_elem
      FROM cte_views_meta
  ), cte_source_meta_only AS (
    SELECT
        b.source_id,
        b.view_id,
        a.meta_elem
      FROM cte_sources_meta_elems AS a
        INNER JOIN cte_views_meta_elems AS b
          USING (source_id)
    EXCEPT
    SELECT
        b.source_id,
        b.view_id,
        b.meta_elem
      FROM cte_views_meta_elems AS b
  ), cte_view_meta_only AS (
    SELECT
        b.source_id,
        b.view_id,
        b.meta_elem
      FROM cte_views_meta_elems AS b
    EXCEPT
    SELECT
        b.source_id,
        b.view_id,
        a.meta_elem
      FROM cte_sources_meta_elems AS a
        INNER JOIN cte_views_meta_elems AS b
          USING (source_id)
  )
    SELECT
        source_id,
        view_id,
        jsonb_agg(DISTINCT b.meta_elem)
          FILTER (WHERE b.meta_elem IS NOT NULL) AS source_metadata_only,
        jsonb_agg(DISTINCT c.meta_elem)
          FILTER (WHERE c.meta_elem IS NOT NULL) AS view_metadata_only
      FROM cte_views_meta AS a
        FULL OUTER JOIN cte_source_meta_only AS b
          USING (source_id, view_id)
        FULL OUTER JOIN cte_view_meta_only AS c
          USING (source_id, view_id)
      GROUP BY source_id, view_id
;


CREATE OR REPLACE VIEW _data_manager_admin.dama_views_metadata_variance AS
  SELECT
      source_id,
      column_name,
      COUNT(DISTINCT type_instances->>'metadata_type') AS distinct_types_count,
      jsonb_agg(
        type_instances ORDER BY type_instances->>'metadata_type'
      ) AS type_instances
    FROM (
      SELECT
          source_id,
          metadata->>'name' AS column_name,
          jsonb_build_object(
            'metadata_type',
            metadata->>'type',

            'view_ids',
            jsonb_agg(view_id ORDER BY 1)
          ) AS type_instances
        FROM (
          SELECT
              a.source_id,
              a.id as view_id,
              jsonb_array_elements(b.table_simplified_schema) AS metadata
            FROM data_manager.views AS a
              INNER JOIN _data_manager_admin.table_json_schema AS b
                USING (table_schema, table_name)
        ) AS t
        GROUP BY source_id, metadata->>'name', metadata->>'type'
    ) AS t
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
              WHERE ( id = %L )
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
              WHERE ( id = %L )
            ',
            v_source_id
          )
        ) INTO v_source_has_meta ;

        IF (v_source_has_meta) THEN
          RAISE EXCEPTION 'DataManager Source metadata MUST be null to initialize.' ;
        END IF;

        WITH cte_src_meta AS (
          SELECT
              b.source_id,
              a.table_simplified_schema AS metadata
            FROM _data_manager_admin.table_json_schema AS a
              INNER JOIN (
                SELECT
                    source_id,
                    table_schema,
                    table_name
                  FROM data_manager.views
                  WHERE (
                    ( id = p_view_id )
                  )
              ) AS b USING (table_schema, table_name)
        )
          UPDATE data_manager.sources AS a
            SET metadata = b.metadata
            FROM cte_src_meta AS b
            WHERE (
              ( a.id = b.source_id )
              AND
              ( a.metadata IS NULL )
            )
        ;

    END ;

  $$;
