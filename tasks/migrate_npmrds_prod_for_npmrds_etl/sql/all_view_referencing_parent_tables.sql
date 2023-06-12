SELECT
    view_schema AS schema_name,
    view_name,
    table_schema,
    table_name
FROM information_schema.view_table_usage
--  WHERE ( table_name = 'npmrds' )
WHERE (
  ( table_name = 'npmrds' )
  OR
  ( table_name ~ '^tmc_identification_\d{4}$' )
)
ORDER BY view_schema, view_name;

/*
$ psql -dnpmrds_production -f all_view_referencing_parent_tables.sql 

 schema_name | view_name | table_schema | table_name 
-------------+-----------+--------------+------------
(0 rows)
*/
