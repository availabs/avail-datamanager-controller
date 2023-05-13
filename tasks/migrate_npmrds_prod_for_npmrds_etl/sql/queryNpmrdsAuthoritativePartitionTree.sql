-- SEE: https://dba.stackexchange.com/a/40614

WITH RECURSIVE cte_inheritance_tree AS (
  SELECT
      NULL::TEXT        AS parent_table_schema,
      NULL::TEXT        AS parent_table_name,
      'public'::NAME    AS table_schema,
      'npmrds'::NAME    AS table_name,

      0 AS depth

  UNION

  SELECT
      d.nspname AS parent_table_schema,
      b.relname AS parent_table_name,
      e.nspname AS table_schema,
      c.relname AS table_name,

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
          ( d.nspname = r.table_schema )
          AND
          ( b.relname = r.table_name )
        )
)
  SELECT
      *
    FROM cte_inheritance_tree
    ORDER BY depth, table_schema, table_name
;
