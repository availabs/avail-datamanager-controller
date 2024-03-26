\timing on
\set ON_ERROR_STOP on

\ir env.sql

-- DROP SCHEMA IF EXISTS :OUTPUT_SCHEMA CASCADE ;

\ir 00_copy_output_table_to_work_schema.sql
\ir 01_create_unnested_input_ids.sql
\ir 02_create_lcgu_union_input_meta.sql
\ir 03_create_lcgu_poly_lineage.sql
\ir 04_create_lcgu_poly_ancestor_properties.sql
\ir 05_create_total_building_footprint_per_parcel.sql
\ir 06_01_create_lcgu_bldg_poly_value.sql
\ir 06_02_qa_create_lcgu_bldg_poly_value.sql
\ir 07_create_lcgu_bld_poly_value_with_geoms.sql
