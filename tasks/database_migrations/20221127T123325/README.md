## Output

```sh
$ psql -hpluto.availabs.org -p5432 -Unpmrds_admin -dnpmrds_production -f preparation.sql

BEGIN
psql:preparation.sql:5: NOTICE:  schema "tmp_dama_migration" does not exist, skipping
DROP SCHEMA
CREATE SCHEMA
SELECT 64
SELECT 389
psql:preparation.sql:22: NOTICE:  drop cascades to 4 other objects
DETAIL:  drop cascades to table data_manager.sources
drop cascades to table data_manager.views
drop cascades to default value for column id of table _data_manager_admin.views_copy
drop cascades to view _transcom_admin.etl_health
DROP SCHEMA
psql:preparation.sql:23: NOTICE:  drop cascades to 9 other objects
DETAIL:  drop cascades to function _data_manager_admin.table_schema_as_json_schema(text,text)
drop cascades to table _data_manager_admin.geojson_json_schemas
drop cascades to view _data_manager_admin.table_column_types
drop cascades to view _data_manager_admin.table_column_types_with_json_types
drop cascades to view _data_manager_admin.table_json_schema
drop cascades to function _data_manager_admin.initialize_sources_metadata(text)
drop cascades to function _data_manager_admin.etl_health_accumulator_fn()
drop cascades to view _data_manager_admin.etl_health_statuses
drop cascades to table _data_manager_admin.views_copy
DROP SCHEMA
COMMIT

$ psql -hpluto.availabs.org -p5432 -Unpmrds_admin -dnpmrds_production -f transfer.sql

BEGIN
INSERT 0 64
INSERT 0 389
COMMIT
```
