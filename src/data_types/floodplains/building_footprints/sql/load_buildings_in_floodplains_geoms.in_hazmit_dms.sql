/*
hazmit_dms=# select public.ST_SRID(footprint), count(1) from irvs.enhanced_building_risk_geom group by 1 order by 2 desc;
 st_srid |  count  
---------+---------
    4326 | 5276891
(1 row)

hazmit_dms=# select public.ST_GeometryType(footprint), count(1) from irvs.enhanced_building_risk_geom group by 1 order by 2 desc;
 st_geometrytype |  count  
-----------------+---------
 ST_Polygon      | 3784889
 ST_MultiPolygon | 1492002
(2 rows)

hazmit_dms=# select not exists (select building_id from irvs.enhanced_building_risk_geom group by building_id having count(1) > 1) as passed;
 passed 
--------
 t
(1 row)
*/

BEGIN ;

INSERT INTO floodplains_pjt.buildings_in_floodplains (
  building_id,
  county,
  floodzone_map_type,
  floodzone_gfid,
  e_bld_rsk_flood_zone,
  map_fld_zone,
  map_zone_subty,
  census_2020_tract_fips,
  nri_2020_avln_eals,
  nri_2020_cfld_eals,
  nri_2020_cwav_eals,
  nri_2020_drgt_eals,
  nri_2020_erqk_eals,
  nri_2020_hail_eals,
  nri_2020_hwav_eals,
  nri_2020_hrcn_eals,
  nri_2020_istm_eals,
  nri_2020_lnds_eals,
  nri_2020_ltng_eals,
  nri_2020_rfld_eals,
  nri_2020_swnd_eals,
  nri_2020_trnd_eals,
  nri_2020_tsun_eals,
  nri_2020_vlcn_eals,
  nri_2020_wfir_eals,
  nri_2020_wntw_eals,
  wkb_geometry
)
  SELECT
      building_id,
      a.county,
      a.floodzone_map_type,
      a.floodzone_gfid,
      a.e_bld_rsk_flood_zone,
      a.map_fld_zone,
      a.map_zone_subty,
      a.census_2020_tract_fips,
      a.nri_2020_avln_eals,
      a.nri_2020_cfld_eals,
      a.nri_2020_cwav_eals,
      a.nri_2020_drgt_eals,
      a.nri_2020_erqk_eals,
      a.nri_2020_hail_eals,
      a.nri_2020_hwav_eals,
      a.nri_2020_hrcn_eals,
      a.nri_2020_istm_eals,
      a.nri_2020_lnds_eals,
      a.nri_2020_ltng_eals,
      a.nri_2020_rfld_eals,
      a.nri_2020_swnd_eals,
      a.nri_2020_trnd_eals,
      a.nri_2020_tsun_eals,
      a.nri_2020_vlcn_eals,
      a.nri_2020_wfir_eals,
      a.nri_2020_wntw_eals,
      ( ST_Multi(b.footprint) )::public.geometry(MultiPolygon,4326) AS wkb_geometry
    FROM floodplains_pjt.buildings_in_floodplains_metadata AS a
      INNER JOIN irvs.enhanced_building_risk_geom AS b
        USING (building_id)
;

CREATE INDEX IF NOT EXISTS buildings_in_floodplains_gidx
  ON floodplains_pjt.buildings_in_floodplains
  USING GIST (wkb_geometry)
;

CLUSTER floodplains_pjt.buildings_in_floodplains
  USING buildings_in_floodplains_pkey
;

DROP TABLE IF EXISTS floodplains_pjt.buildings_in_floodplains_metadata ;

COMMIT ;
