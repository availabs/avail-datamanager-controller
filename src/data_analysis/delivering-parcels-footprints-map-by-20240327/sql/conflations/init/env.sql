\set PARCELS_TABLE_SCHEMA delivering_parcels_footprints
\set PARCELS_TABLE_NAME parcels_albany_co

SELECT
    MAX(view_id) AS "PARCELS_VIEW_ID"
  FROM data_manager.views
  WHERE (
    ( table_schema = :'PARCELS_TABLE_SCHEMA' )
    AND
    ( table_name = :'PARCELS_TABLE_NAME' )
  )
\gset

\set NYS_ITS_FOOTPRINTS_TABLE_SCHEMA delivering_parcels_footprints
\set NYS_ITS_FOOTPRINTS_TABLE_NAME nys_its_footprints_albany_co

SELECT
    MAX(view_id) AS "NYS_ITS_FOOTPRINTS_VIEW_ID"
  FROM data_manager.views
  WHERE (
    ( table_schema = :'NYS_ITS_FOOTPRINTS_TABLE_SCHEMA' )
    AND
    ( table_name = :'NYS_ITS_FOOTPRINTS_TABLE_NAME' )
  )
\gset

\set UNION_TABLE_SCHEMA delivering_parcels_footprints
\set UNION_TABLE_NAME parcels_u_nys_footprints_albany_co

\set LCGU_TABLE_SCHEMA __tmp_etl_work_schema_eci_137__
\set LCGU_TABLE_NAME overlap_free_polygons

\set OUTPUT_SCHEMA lcgu_view_116_conflation


----- Show the ENV

\echo
\echo 'PARCELS_TABLE_SCHEMA             ':PARCELS_TABLE_SCHEMA
\echo 'PARCELS_TABLE_NAME               ':PARCELS_TABLE_NAME
\echo 'PARCELS_VIEW_ID                  ':PARCELS_VIEW_ID
\echo
\echo 'NYS_ITS_FOOTPRINTS_TABLE_SCHEMA  ':NYS_ITS_FOOTPRINTS_TABLE_SCHEMA
\echo 'NYS_ITS_FOOTPRINTS_TABLE_NAME    ':NYS_ITS_FOOTPRINTS_TABLE_NAME
\echo 'NYS_ITS_FOOTPRINTS_VIEW_ID       ':NYS_ITS_FOOTPRINTS_VIEW_ID
\echo
\echo 'UNION_TABLE_SCHEMA               ':UNION_TABLE_SCHEMA
\echo 'UNION_TABLE_NAME                 ':UNION_TABLE_NAME
\echo
\echo 'LCGU_TABLE_SCHEMA                ':LCGU_TABLE_SCHEMA
\echo 'LCGU_TABLE_NAME                  ':LCGU_TABLE_NAME
\echo
\echo 'OUTPUT_SCHEMA                    ':OUTPUT_SCHEMA
\echo
