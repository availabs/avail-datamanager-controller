COPY floodplains_pjt.buildings_in_floodplains_metadata (
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
    nri_2020_wntw_eals
  ) 
  FROM '/home/avail/dbadmin/floodplains/buildings_in_floodplains_metadata.csv'
  WITH CSV HEADER
;
