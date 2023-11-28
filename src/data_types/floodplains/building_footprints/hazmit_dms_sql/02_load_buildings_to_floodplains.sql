\timing on
\set ON_ERROR_STOP true

BEGIN ;

DROP TABLE IF EXISTS floodplains_work_schema.buildings_to_floodplains ;
CREATE TABLE floodplains_work_schema.buildings_to_floodplains (
  building_id           BIGINT,
  floodzone_map_type    TEXT NOT NULL,
  floodzone_gfid        TEXT NOT NULL,
  e_bld_rsk_flood_zone  TEXT,
  map_fld_zone          TEXT,
  map_zone_subty        TEXT,
  wkb_geometry          public.geometry(MultiPolygon, 4326) NOT NULL,

  PRIMARY KEY (building_id, floodzone_gfid)
) ;

-- Initial load of buildings_to_floodplains. WARNING: TABLE WILL HAVE DUPLICATE ENTRIES FOR BUILDINGS!

INSERT INTO floodplains_work_schema.buildings_to_floodplains (
  building_id,
  floodzone_map_type,
  floodzone_gfid,
  e_bld_rsk_flood_zone,
  map_fld_zone,
  map_zone_subty,
  wkb_geometry
)
  SELECT DISTINCT ON (building_id, floodzone_gfid)
      a.building_id,
      REGEXP_REPLACE(c.gfid, ':.*', '') AS floodzone_map_type,
      c.gfid AS floodzone_gfid,
      a.flood_zone AS e_bld_rsk_flood_zone,
      c.fld_zone AS map_fld_zone,
      c.zone_subty AS map_zone_subty,
      ST_Multi(a.geom) AS wkb_geometry
    FROM irvs.enhanced_building_risk_geom AS a
      INNER JOIN floodplains_work_schema.interesting_merged_all_with_split_geoms AS b
        ON ( ST_Intersects(a.geom, b.wkb_geometry ) )
      INNER JOIN floodplains_work_schema.merged_all AS c
        ON ( b.id = c.fid )
    ORDER BY building_id, floodzone_gfid
;

COMMIT ;
