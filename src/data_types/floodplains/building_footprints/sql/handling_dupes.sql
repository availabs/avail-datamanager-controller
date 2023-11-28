/*
SELECT
    map_fld_zone,
    map_zone_subty,
    COUNT(1)
  FROM floodplains.buildings_in_floodplains AS a
    INNER JOIN (
      SELECT
          building_id
        FROM floodplains.buildings_in_floodplains
        GROUP BY building_id
        HAVING ( COUNT(1) > 1 )
    ) AS b
      USING ( building_id )
  GROUP BY 1,2
  ORDER BY 3 DESC
;

 map_fld_zone |             map_zone_subty              | count
--------------+-----------------------------------------+-------
 X            | 0.2 PCT ANNUAL CHANCE FLOOD HAZARD      | 60570
 AE           |                                         | 49955
 A            |                                         | 26013
 AE           | FLOODWAY                                |  2876
 VE           |                                         |  1887
 X            | 1 PCT DEPTH LESS THAN 1 FOOT            |   752
 AO           |                                         |   196
 AE           | FW                                      |   144
 AH           |                                         |   110
 VE           | COASTAL FLOODPLAIN                      |    33
 AE           | RIVERINE FLOODWAY SHOWN IN COASTAL ZONE |    22
 AE           | COASTAL FLOODPLAIN                      |     5

RANKING
 1  |  AE  | FLOODWAY                                
 2  |  VE  | COASTAL FLOODPLAIN                      
 3  |  AE  | RIVERINE FLOODWAY SHOWN IN COASTAL ZONE 
 4  |  AE  | COASTAL FLOODPLAIN                      
 5  |  AE  |                                         
 6  |  AE  | FW                                      
 7  |  A   |                                         
 8  |  VE  |                                         
 9  |  AH  |                                         
10  |  AO  |                                         
11  |  X   | 0.2 PCT ANNUAL CHANCE FLOOD HAZARD      
12  |  X   | 1 PCT DEPTH LESS THAN 1 FOOT 
*/

/*
-- This is a simple example of what the dupe-removal does.
CREATE TEMPORARY TABLE tmp_foo
  AS
    SELECT
        *
      FROM (
        VALUES
        (  1,  'PRELIMINARY'  ),
        (  2,  'NHFL'         ),
        (  3,  'BLE'          ),
        (  4,  'Q3'           )
      ) AS t(rank, type)
;

CREATE TEMPORARY TABLE tmp_bar
  AS
    SELECT
        *
      FROM (
        VALUES
        (  1,  'PRELIMINARY'  ),
        (  2,  'NOT NHFL'     )
      ) AS t(rank, type)
;


DELETE FROM tmp_foo
  WHERE (  
    ( rank IN ( SELECT rank FROM tmp_bar ) )
    AND
    ( (rank, type) NOT IN (SELECT rank, type FROM tmp_bar) )
  )
;

SELECT * FROM tmp_foo;

// OUTPUT:
//   SELECT 4
//   Time: 3.216 ms
//   SELECT 2
//   Time: 1.695 ms
//   DELETE 1
//   Time: 0.641 ms
//    rank |    type
//   ------+-------------
//       1 | PRELIMINARY
//       3 | BLE
//       4 | Q3
//   (3 rows)
*/


\timing on
\set ON_ERROR_STOP true

/*
BEGIN ;

DROP TABLE IF EXISTS floodplains.tmp_dupe_building_ids ;
CREATE TABLE floodplains.tmp_dupe_building_ids (
  building_id     INTEGER PRIMARY KEY
) ;

INSERT INTO floodplains.tmp_dupe_building_ids
  SELECT
      building_id
    FROM floodplains.buildings_in_floodplains
    GROUP BY building_id
    HAVING ( COUNT(1) > 1 )
;

CLUSTER floodplains.tmp_dupe_building_ids USING tmp_dupe_building_ids_pkey ;

DROP TABLE IF EXISTS floodplains.flood_map_rankings ;
CREATE TABLE floodplains.flood_map_rankings AS
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


DROP TABLE IF EXISTS floodplains.flood_zone_rankings ;
CREATE TABLE floodplains.flood_zone_rankings AS
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

DROP TABLE IF EXISTS floodplains.tmp_building_dupes_to_keep ;
CREATE TABLE floodplains.tmp_building_dupes_to_keep (
  building_id     INTEGER PRIMARY KEY,
  floodzone_gfid  TEXT NOT NULL
) ;

-- CREATE OR REPLACE VIEW floodplains.tmp_building_dupes_to_delete
--   AS
--     SELECT
--         building_id,
--         floodzone_gfid
--       FROM floodplains.tmp_building_dupes
--     EXCEPT
--     SELECT
--         building_id,
--         floodzone_gfid
--       FROM floodplains.tmp_building_dupes_to_keep
-- ;


INSERT INTO floodplains.tmp_building_dupes_to_keep (
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
      FROM floodplains.tmp_dupe_building_ids
        INNER JOIN floodplains.buildings_in_floodplains AS b
          USING ( building_id )
        INNER JOIN floodplains.flood_map_rankings AS c
          ON ( b.floodzone_map_type = c.flood_map_type )
        INNER JOIN floodplains.flood_zone_rankings AS d
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

DROP TABLE IF EXISTS floodplains.tmp_building_dupes ;
CREATE TABLE floodplains.tmp_building_dupes
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
            FROM floodplains.tmp_dupe_building_ids
          EXCEPT
          SELECT
              building_id
            FROM floodplains.tmp_building_dupes_to_keep
        ) AS a
        INNER JOIN floodplains.buildings_in_floodplains AS b
          USING ( building_id )
        INNER JOIN floodplains.interesting_merged_all_with_split_geoms AS c
          ON ( b.floodzone_gfid = c.gfid )
        INNER JOIN floodplains.flood_map_rankings AS d
          ON ( b.floodzone_map_type = d.flood_map_type )
        INNER JOIN floodplains.flood_zone_rankings AS e
          ON (
            ( b.map_fld_zone = e.map_fld_zone )
            AND
            ( COALESCE(b.map_zone_subty, '') = COALESCE(e.map_zone_subty, '') )
          )
      GROUP BY 1,2,3,4
;

INSERT INTO floodplains.tmp_building_dupes_to_keep (
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
      FROM floodplains.tmp_building_dupes
  )
    SELECT
        building_id,
        floodzone_gfid
      FROM cte_ranked
      WHERE ( floodplain_rank = 1 )
;

*/

CREATE TABLE floodplains.test (id INT) ;

CREATE TEMPORARY TABLE tmp_num_buildings_before
  ON COMMIT DROP
  AS
    SELECT
        COUNT(DISTINCT building_id) AS num_buildings
      FROM floodplains.buildings_in_floodplains
;

DELETE FROM floodplains.buildings_in_floodplains
 WHERE (
   ( building_id IN (
       SELECT
           building_id
         FROM floodplains.tmp_dupe_building_ids 
     )
   )
   AND
   ( (building_id, floodzone_gfid) NOT IN (
       SELECT
           building_id,
           floodzone_gfid
         FROM floodplains.tmp_building_dupes_to_keep
     )
   )
 )
;

CREATE TEMPORARY TABLE tmp_num_rows_after
  ON COMMIT DROP
  AS
    SELECT
        COUNT(1) AS num_rows
      FROM floodplains.buildings_in_floodplains
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
