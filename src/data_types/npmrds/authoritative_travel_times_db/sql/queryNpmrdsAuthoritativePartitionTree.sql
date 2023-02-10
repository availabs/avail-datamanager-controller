-- SEE: https://dba.stackexchange.com/a/40614

WITH RECURSIVE cte_inheritance_tree AS (
  SELECT
      d.nspname AS parent_table_schema,
      b.relname AS parent_table_name,
      e.nspname AS child_table_schema,
      c.relname AS child_table_name,

      0 AS depth

    FROM pg_namespace AS d

      INNER JOIN pg_class AS b
        ON ( d.oid = b.relnamespace )

      INNER JOIN pg_inherits AS a
        ON ( a.inhparent = b.oid )

      INNER JOIN pg_class AS c
        ON ( a.inhrelid = c.oid )

      INNER JOIN pg_namespace AS e
        ON ( e.oid = c.relnamespace )

    WHERE (
      ( d.nspname = 'public' )
      AND
      ( b.relname = 'npmrds_test' )
    )

  UNION

  SELECT
      d.nspname AS parent_table_schema,
      b.relname AS parent_table_name,
      e.nspname AS child_table_schema,
      c.relname AS child_table_name,

      r.depth + 1 AS depth

    FROM pg_namespace AS d

      INNER JOIN pg_class AS b
        ON ( d.oid = b.relnamespace )

      INNER JOIN pg_inherits AS a
        ON ( a.inhparent = b.oid )

      INNER JOIN pg_class AS c
        ON ( a.inhrelid = c.oid )

      INNER JOIN pg_namespace AS e
        ON ( e.oid = c.relnamespace )

      INNER JOIN cte_inheritance_tree AS r
        ON (
          ( d.nspname = r.child_table_schema )
          AND
          ( b.relname = r.child_table_name )
        )
)
  SELECT
      b.*
    FROM cte_inheritance_tree AS a
      INNER JOIN data_manager.views AS b
        ON (
          ( a.child_table_schema = b.table_schema )
          AND
          ( a.child_table_name = b.table_name )
        )
      INNER JOIN data_manager.sources AS c
        USING ( source_id )
    WHERE ( c.name = %L ) -- NpmrdsTravelTimesExportDb DamaSource Name
;
