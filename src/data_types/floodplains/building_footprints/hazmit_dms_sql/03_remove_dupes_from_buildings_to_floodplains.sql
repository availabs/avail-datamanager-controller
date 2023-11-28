\timing on
\set ON_ERROR_STOP true

BEGIN ;
-- Remove dupes from floodplains_work_schema.buildings_to_floodplains

--   Which building_ids occur more than once in floodplains_work_schema.buildings_to_floodplains?

DROP TABLE IF EXISTS floodplains_work_schema.tmp_dupe_building_ids ;
CREATE TABLE floodplains_work_schema.tmp_dupe_building_ids (
  building_id     INTEGER PRIMARY KEY
) ;

INSERT INTO floodplains_work_schema.tmp_dupe_building_ids
  SELECT
      building_id
    FROM floodplains_work_schema.buildings_in_floodplains
    GROUP BY building_id
    HAVING ( COUNT(1) > 1 )
;

CLUSTER floodplains_work_schema.tmp_dupe_building_ids USING tmp_dupe_building_ids_pkey ;

--  tmp_building_dupes_to_keep stores the (building_id, floodzone_gfid) pairs to keep.

DROP TABLE IF EXISTS floodplains_work_schema.tmp_building_dupes_to_keep ;
CREATE TABLE floodplains_work_schema.tmp_building_dupes_to_keep (
  building_id     INTEGER PRIMARY KEY,
  floodzone_gfid  TEXT NOT NULL
) ;

INSERT INTO floodplains_work_schema.tmp_building_dupes_to_keep (
  building_id,
  floodzone_gfid
)
  WITH cte_ranked AS (
    SELECT
        building_id,
        floodzone_gfid,
        flood_map_rank,
        flood_zone_rank,
        rank() OVER (PARTITION BY building_id ORDER BY flood_map_rank, flood_zone_rank) AS floodplain_rank
      FROM floodplains_work_schema.tmp_dupe_building_ids
        INNER JOIN floodplains_work_schema.buildings_in_floodplains AS b
          USING ( building_id )
        INNER JOIN floodplains_work_schema.flood_map_rankings AS c
          ON ( b.floodzone_map_type = c.flood_map_type )
        INNER JOIN floodplains_work_schema.flood_zone_rankings AS d
          ON (
            ( b.map_fld_zone = d.map_fld_zone )
            AND
            ( COALESCE(b.map_zone_subty, '') = COALESCE(d.map_zone_subty, '') )
          )
      ORDER BY building_id, flood_map_rank, flood_zone_rank
  ), cte_keepers AS (
    SELECT
        building_id
      FROM cte_ranked
      WHERE (floodplain_rank = 1)
      GROUP BY building_id
      HAVING ( COUNT(1) = 1 )
  )
    SELECT
        a.building_id,
        a.floodzone_gfid
      FROM cte_ranked AS a
        INNER JOIN cte_keepers AS b
          USING (building_id)
      WHERE ( a.floodplain_rank = 1 )
;

DROP TABLE IF EXISTS floodplains_work_schema.tmp_building_dupes_with_intxn_area ;
CREATE TABLE floodplains_work_schema.tmp_building_dupes_with_intxn_area
  AS
    SELECT
        a.building_id,
        b.floodzone_gfid,
        d.flood_map_rank,
        e.flood_zone_rank,
        ST_Area(
          ST_UnaryUnion(
            ST_Collect(
              ST_Intersection(
                b.wkb_geometry,
                c.wkb_geometry
              )
            )
          )
        ) AS flood_zone_intersection_area_4326

      FROM (
          SELECT
              building_id
            FROM floodplains_work_schema.tmp_dupe_building_ids
          EXCEPT
          SELECT
              building_id
            FROM floodplains_work_schema.tmp_building_dupes_to_keep
        ) AS a
        INNER JOIN floodplains_work_schema.buildings_in_floodplains AS b
          USING ( building_id )
        INNER JOIN floodplains_work_schema.interesting_merged_all_with_split_geoms AS c
          ON ( b.floodzone_gfid = c.gfid )
        INNER JOIN floodplains_work_schema.flood_map_rankings AS d
          ON ( b.floodzone_map_type = d.flood_map_type )
        INNER JOIN floodplains_work_schema.flood_zone_rankings AS e
          ON (
            ( b.map_fld_zone = e.map_fld_zone )
            AND
            ( COALESCE(b.map_zone_subty, '') = COALESCE(e.map_zone_subty, '') )
          )
      GROUP BY 1,2,3,4
;

INSERT INTO floodplains_work_schema.tmp_building_dupes_to_keep (
  building_id,
  floodzone_gfid
)
  WITH cte_ranked AS (
    SELECT
        building_id,
        floodzone_gfid,
        flood_map_rank,
        flood_zone_rank,
        row_number() OVER (
          PARTITION BY building_id 
          ORDER BY
              flood_map_rank,
              flood_zone_rank,
              flood_zone_intersection_area_4326 DESC,
              floodzone_gfid
        ) AS floodplain_rank
      FROM floodplains_work_schema.tmp_building_dupes_with_intxn_area
  )
    SELECT
        building_id,
        floodzone_gfid
      FROM cte_ranked
      WHERE ( floodplain_rank = 1 )
;

CREATE TEMPORARY TABLE tmp_num_buildings_before
  ON COMMIT DROP
  AS
    SELECT
        COUNT(DISTINCT building_id) AS num_buildings
      FROM floodplains_work_schema.buildings_in_floodplains
;

DELETE FROM floodplains_work_schema.buildings_in_floodplains
 WHERE (
   ( building_id IN (
       SELECT
           building_id
         FROM floodplains_work_schema.tmp_dupe_building_ids 
     )
   )
   AND
   ( (building_id, floodzone_gfid) NOT IN (
       SELECT
           building_id,
           floodzone_gfid
         FROM floodplains_work_schema.tmp_building_dupes_to_keep
     )
   )
 )
;

CREATE TEMPORARY TABLE tmp_num_rows_after
  ON COMMIT DROP
  AS
    SELECT
        COUNT(1) AS num_rows
      FROM floodplains_work_schema.buildings_in_floodplains
;

DO $$
BEGIN
  IF (
    (SELECT num_buildings FROM tmp_num_buildings_before)
    !=
    (SELECT num_rows FROM tmp_num_rows_after)
  ) THEN
      RAISE EXCEPTION 'Something went wrong removing dupes. ROLLBACK!' ;
  END IF;  
END$$;

COMMIT ;
