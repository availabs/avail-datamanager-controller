BEGIN ;

DROP TABLE IF EXISTS floodplains.buildings_in_floodplains ;

CREATE TABLE floodplains.buildings_in_floodplains (
  building_id           BIGINT,
  floodzone_map_type    TEXT NOT NULL,
  floodzone_gfid        TEXT NOT NULL,
  e_bld_rsk_flood_zone  TEXT,
  map_fld_zone          TEXT,
  map_zone_subty        TEXT,
  wkb_geometry          public.geometry(MultiPolygon, 4326) NOT NULL,

  PRIMARY KEY (building_id, floodzone_gfid)
) ;

INSERT INTO floodplains.buildings_in_floodplains (
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
    FROM floodplains.enhanced_building_risk_geom AS a
      INNER JOIN floodplains.interesting_merged_all_with_split_geoms AS b
        ON ( ST_Intersects(a.geom, b.wkb_geometry ) )
      INNER JOIN floodplains.merged_all AS c
        ON ( b.id = c.fid )
    ORDER BY building_id, floodzone_gfid
;

COMMIT ;
