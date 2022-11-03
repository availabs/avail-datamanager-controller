# DataManager Database Tables, Views, and Functions

## Governance

The `_data_manager_admin` schema contains some VIEWs that will help enforce
conformity between a DamaSource's metadata and its DamaViews' table schemas.

### \_data_manager_admin.dama_source_distinct_view_metadata

For each source, shows the consistency of views metadata.

The `distinct_view_metadata_count` value SHOULD be 1.

```sql
dama_dev_1=# \d dama_source_distinct_view_metadata
      View "_data_manager_admin.dama_source_distinct_view_metadata"
            Column            |  Type   | Collation | Nullable | Default
------------------------------+---------+-----------+----------+---------
 source_id                    | integer |           |          |
 distinct_view_metadata_count | bigint  |           |          |
 views_metadata_summary       | jsonb   |           |          |
```

### \_data_manager_admin.dama_source_distinct_view_table_schemas

For each DamaSource, shows the consistency of views table schemas.

The `distinct_view_table_schemas` value SHOULD be 1.

```sql
dama_dev_1=# \d dama_source_distinct_view_table_schemas
   View "_data_manager_admin.dama_source_distinct_view_table_schemas"
           Column            |  Type   | Collation | Nullable | Default
-----------------------------+---------+-----------+----------+---------
 source_id                   | integer |           |          |
 distinct_view_table_schemas | bigint  |           |          |
 table_schemas_summary       | jsonb   |           |          |
```

### \_data_manager_admin.dama_views_column_type_variance

For each DamaSource, shows the consistency of DamaViews' table columns.

The `distinct_db_types_count` and `distinct_meta_types_count` values SHOULD be 1.

```sql
dama_dev_1=# \d dama_views_column_type_variance
      View "_data_manager_admin.dama_views_column_type_variance"
          Column           |  Type   | Collation | Nullable | Default
---------------------------+---------+-----------+----------+---------
 source_id                 | integer |           |          |
 column_name               | text    |           |          |
 distinct_db_types_count   | bigint  |           |          |
 distinct_meta_types_count | bigint  |           |          |
 db_type_instances         | jsonb   |           |          |
 meta_type_instances       | jsonb   |           |          |
```

### \_data_manager_admin.dama_views_metadata_conformity

For each DamaView, compare the (data table derived) view metadata
with the DamaSource metadata.

The `source_metadata_only` and `view_metadata_only` values SHOULD be null.
The `view_metadata_is_comformant` value SHOULD be true.

```sql
dama_dev_1=# \d dama_views_metadata_conformity
       View "_data_manager_admin.dama_views_metadata_conformity"
           Column            |  Type   | Collation | Nullable | Default
-----------------------------+---------+-----------+----------+---------
 source_id                   | integer |           |          |
 view_id                     | integer |           |          |
 source_metadata_only        | jsonb   |           |          |
 view_metadata_only          | jsonb   |           |          |
 view_metadata_is_comformant | boolean |           |          |
```

### \_data_manager_admin.dama_views_missing_tables

Shows the DamaView where the data_table does not exist.

```sql
dama_dev_1=# \d dama_views_missing_tables
 View "_data_manager_admin.dama_views_missing_tables"
  Column   |  Type   | Collation | Nullable | Default
-----------+---------+-----------+----------+---------
 source_id | integer |           |          |
 view_id   | integer |           |          |
```
