\timing on
\set ON_ERROR_STOP true

BEGIN ;

CREATE SCHEMA IF NOT EXISTS floodplains_work_schema ;

CREATE OR REPLACE VIEW floodplains_work_schema.merged_all
  AS
    SELECT
        *
      FROM gis_datasets.s379_v841_avail_nys_floodplains_merged
;

-- Create is_interesting function.

DROP FUNCTION IF EXISTS floodplains_work_schema.is_interesting CASCADE;
CREATE FUNCTION floodplains_work_schema.is_interesting(/*fld_zone*/ TEXT, /*zone_subty*/ TEXT)
  RETURNS BOOLEAN
  AS $$
    SELECT (
          ( COALESCE($1, '') != 'AREA NOT INCLUDED' )
          AND
          (
            ( COALESCE($1, '') != 'X' )
            OR
            (
              COALESCE($2, '')
                IN (
                  '0.2 PCT ANNUAL CHANCE FLOOD HAZARD', 
                  '1 PCT DEPTH LESS THAN 1 FOOT'
                )
            )
          )
        )
  $$
  LANGUAGE SQL
  IMMUTABLE
;

-- Create the interesting_merged_all_with_split_geoms table.

CREATE VIEW floodplains_work_schema.interesting_merged_all
  AS
    SELECT
        *
      FROM floodplains_work_schema.merged_all
      WHERE ( floodplains_work_schema.is_interesting(fld_zone, zone_subty) )
;

-- Create the interesting_merged_all_with_split_geoms table. This table is optimized for spatial joins.

DROP TABLE IF EXISTS floodplains_work_schema.interesting_merged_all_with_split_geoms ;
CREATE TABLE floodplains_work_schema.interesting_merged_all_with_split_geoms (
  id              INTEGER NOT NULL,
  gfid            TEXT NOT NULL,
  wkb_geometry    public.geometry(Polygon,4326)
) ;

INSERT INTO floodplains_work_schema.interesting_merged_all_with_split_geoms (id, gfid, wkb_geometry)
  SELECT
      ogc_fid AS id,
      gfid,
      ST_Subdivide(wkb_geometry) AS wkb_geometry
    FROM floodplains_work_schema.interesting_merged_all
;

CREATE INDEX interesting_merged_all_divided_geom_gidx
  ON floodplains_work_schema.interesting_merged_all_with_split_geoms
  USING GIST (wkb_geometry)
;

CLUSTER floodplains_work_schema.interesting_merged_all_with_split_geoms
  USING interesting_merged_all_divided_geom_gidx
;

DROP TABLE IF EXISTS floodplains_work_schema.flood_map_rankings ;
CREATE TABLE floodplains_work_schema.flood_map_rankings AS
  SELECT
      *
    FROM (
      VALUES
      (  1,  'PRELIMINARY'  ),
      (  2,  'NHFL'         ),
      (  3,  'BLE'          ),
      (  4,  'Q3'           )
    ) AS t(flood_map_rank, flood_map_type)
;

-- NOTE:  The following is not an exhaustive ranking of the flood zone types.
--        It contains ONLY a subset of the (fld_zone, zone_subty) pairs.
--        The pairs below are limited to what is required to resolve
--          duplicate buildings from floodplains_work_schema.buildings_to_floodplains.

DROP TABLE IF EXISTS floodplains_work_schema.flood_zone_rankings ;
CREATE TABLE floodplains_work_schema.flood_zone_rankings AS
  SELECT
      *
    FROM (
      VALUES
      (  1,  'AE',  'FLOODWAY'                                ),
      (  2,  'VE',  'COASTAL FLOODPLAIN'                      ),
      (  3,  'AE',  'RIVERINE FLOODWAY SHOWN IN COASTAL ZONE' ),
      (  4,  'AE',  'COASTAL FLOODPLAIN'                      ),
      (  5,  'AE',   NULL                                     ),
      (  6,  'AE',  'FW'                                      ),
      (  7,   'A',   NULL                                     ),
      (  8,  'VE',   NULL                                     ),
      (  9,  'AH',   NULL                                     ),
      ( 10,  'AO',   NULL                                     ),
      ( 11,   'X',  '0.2 PCT ANNUAL CHANCE FLOOD HAZARD'      ),
      ( 12,   'X',  '1 PCT DEPTH LESS THAN 1 FOOT'            )
    ) AS t(flood_zone_rank, map_fld_zone, map_zone_subty)
;

COMMIT ;
