DROP VIEW IF EXISTS floodplains.buildings_in_floodplains_metadata ;
CREATE OR REPLACE VIEW floodplains.buildings_in_floodplains_metadata
  AS
    SELECT
        building_id,
        REGEXP_REPLACE(c.namelsad, ' County', '') AS county,

        a.floodzone_map_type,
        a.floodzone_gfid,
        a.e_bld_rsk_flood_zone,
        a.map_fld_zone,
        a.map_zone_subty,

        b.census_tract_fips AS census_2020_tract_fips,

        b.nri_avln_eals AS nri_2020_avln_eals,
        b.nri_cfld_eals AS nri_2020_cfld_eals,
        b.nri_cwav_eals AS nri_2020_cwav_eals,
        b.nri_drgt_eals AS nri_2020_drgt_eals,
        b.nri_erqk_eals AS nri_2020_erqk_eals,
        b.nri_hail_eals AS nri_2020_hail_eals,
        b.nri_hwav_eals AS nri_2020_hwav_eals,
        b.nri_hrcn_eals AS nri_2020_hrcn_eals,
        b.nri_istm_eals AS nri_2020_istm_eals,
        b.nri_lnds_eals AS nri_2020_lnds_eals,
        b.nri_ltng_eals AS nri_2020_ltng_eals,
        b.nri_rfld_eals AS nri_2020_rfld_eals,
        b.nri_swnd_eals AS nri_2020_swnd_eals,
        b.nri_trnd_eals AS nri_2020_trnd_eals,
        b.nri_tsun_eals AS nri_2020_tsun_eals,
        b.nri_vlcn_eals AS nri_2020_vlcn_eals,
        b.nri_wfir_eals AS nri_2020_wfir_eals,
        b.nri_wntw_eals AS nri_2020_wntw_eals
      FROM floodplains.buildings_in_floodplains AS a
        INNER JOIN floodplains.buildings_2020_fema_nri_ealss AS b
          USING (building_id)
        INNER JOIN us_census_tiger.county AS c
          ON ( LEFT(b.census_tract_fips, 5) = c.geoid )
      ORDER BY 2,1
;
