CREATE OR REPLACE FUNCTION _data_manager_admin.etl_health_accumulator_fn ()
    RETURNS TABLE (
      data_manager_view_id      INTEGER,
      expected_update_interval  INTERVAL,
      actual_update_interval    INTERVAL,
      etl_health_status         TEXT
    )

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
  $$ LANGUAGE 'plpgsql'
;

CREATE OR REPLACE VIEW _data_manager_admin.etl_health_statuses
  AS
    SELECT
        *
      FROM _data_manager_admin.etl_health_accumulator_fn()
;
