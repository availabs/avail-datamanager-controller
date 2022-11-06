-- Because falcor needs the tables' PRIMARY KEYs.
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
                v.id AS view_id,
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

