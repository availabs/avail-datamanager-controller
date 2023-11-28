--  This was used to create the floodplains_pjt.buildings_in_floodplains_metadata table
--    in the hazmit_dms database. It was sent to avail@pluto:~/dbadmin/floodplains and
--    executed from there.

BEGIN ;

CREATE SCHEMA IF NOT EXISTS floodplains_pjt ;

CREATE TABLE IF NOT EXISTS floodplains_pjt.buildings_in_floodplains_metadata (
 building_id              BIGINT PRIMARY KEY,
 county                   TEXT,
 floodzone_map_type       TEXT,
 floodzone_gfid           TEXT,
 e_bld_rsk_flood_zone     TEXT,
 map_fld_zone             TEXT,
 map_zone_subty           TEXT,
 census_2020_tract_fips   TEXT,
 nri_2020_avln_eals       DOUBLE PRECISION,
 nri_2020_cfld_eals       DOUBLE PRECISION,
 nri_2020_cwav_eals       DOUBLE PRECISION,
 nri_2020_drgt_eals       DOUBLE PRECISION,
 nri_2020_erqk_eals       DOUBLE PRECISION,
 nri_2020_hail_eals       DOUBLE PRECISION,
 nri_2020_hwav_eals       DOUBLE PRECISION,
 nri_2020_hrcn_eals       DOUBLE PRECISION,
 nri_2020_istm_eals       DOUBLE PRECISION,
 nri_2020_lnds_eals       DOUBLE PRECISION,
 nri_2020_ltng_eals       DOUBLE PRECISION,
 nri_2020_rfld_eals       DOUBLE PRECISION,
 nri_2020_swnd_eals       DOUBLE PRECISION,
 nri_2020_trnd_eals       DOUBLE PRECISION,
 nri_2020_tsun_eals       DOUBLE PRECISION,
 nri_2020_vlcn_eals       DOUBLE PRECISION,
 nri_2020_wfir_eals       DOUBLE PRECISION,
 nri_2020_wntw_eals       DOUBLE PRECISION
) WITH (fillfactor=100) ;

CREATE TABLE IF NOT EXISTS floodplains_pjt.buildings_in_floodplains (
  LIKE floodplains_pjt.buildings_in_floodplains_metadata INCLUDING ALL,
  wkb_geometry public.geometry(MultiPolygon,4326) NOT NULL
) WITH (fillfactor=100) ;

COMMIT ;
