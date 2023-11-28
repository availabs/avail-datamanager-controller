BEGIN ;

DROP TABLE IF EXISTS floodplains.buildings_2020_fema_nri_ealss ;

CREATE TABLE floodplains.buildings_2020_fema_nri_ealss (
  building_id         INTEGER PRIMARY KEY,
  census_tract_fips   TEXT NOT NULL,
  nri_avln_eals       DOUBLE PRECISION,
  nri_cfld_eals       DOUBLE PRECISION,
  nri_cwav_eals       DOUBLE PRECISION,
  nri_drgt_eals       DOUBLE PRECISION,
  nri_erqk_eals       DOUBLE PRECISION,
  nri_hail_eals       DOUBLE PRECISION,
  nri_hwav_eals       DOUBLE PRECISION,
  nri_hrcn_eals       DOUBLE PRECISION,
  nri_istm_eals       DOUBLE PRECISION,
  nri_lnds_eals       DOUBLE PRECISION,
  nri_ltng_eals       DOUBLE PRECISION,
  nri_rfld_eals       DOUBLE PRECISION,
  nri_swnd_eals       DOUBLE PRECISION,
  nri_trnd_eals       DOUBLE PRECISION,
  nri_tsun_eals       DOUBLE PRECISION,
  nri_vlcn_eals       DOUBLE PRECISION,
  nri_wfir_eals       DOUBLE PRECISION,
  nri_wntw_eals       DOUBLE PRECISION
) ;

INSERT INTO floodplains.buildings_2020_fema_nri_ealss (
  building_id,
  census_tract_fips,
  nri_avln_eals,
  nri_cfld_eals,
  nri_cwav_eals,
  nri_drgt_eals,
  nri_erqk_eals,
  nri_hail_eals,
  nri_hwav_eals,
  nri_hrcn_eals,
  nri_istm_eals,
  nri_lnds_eals,
  nri_ltng_eals,
  nri_rfld_eals,
  nri_swnd_eals,
  nri_trnd_eals,
  nri_tsun_eals,
  nri_vlcn_eals,
  nri_wfir_eals,
  nri_wntw_eals
)
  SELECT
      a.building_id,
      a.census_tract_fips,

      b.avln_eals AS nri_avln_eals,
      b.cfld_eals AS nri_cfld_eals,
      b.cwav_eals AS nri_cwav_eals,
      b.drgt_eals AS nri_drgt_eals,
      b.erqk_eals AS nri_erqk_eals,
      b.hail_eals AS nri_hail_eals,
      b.hwav_eals AS nri_hwav_eals,
      b.hrcn_eals AS nri_hrcn_eals,
      b.istm_eals AS nri_istm_eals,
      b.lnds_eals AS nri_lnds_eals,
      b.ltng_eals AS nri_ltng_eals,
      b.rfld_eals AS nri_rfld_eals,
      b.swnd_eals AS nri_swnd_eals,
      b.trnd_eals AS nri_trnd_eals,
      b.tsun_eals AS nri_tsun_eals,
      b.vlcn_eals AS nri_vlcn_eals,
      b.wfir_eals AS nri_wfir_eals,
      b.wntw_eals AS nri_wntw_eals

    FROM floodplains.buildings_to_census_tracts AS a
      INNER JOIN vulnerability.fema_nri_tract_level AS b
        ON ( a.census_tract_fips = b.tractfips )
;

COMMIT ;
