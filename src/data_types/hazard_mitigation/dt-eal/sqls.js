export const eal = (table_name, view_id,
                    hlr_schema, hlr_table,
                    nri_schema, nri_table
) => `
  with dollars as (SELECT nri_category,
                          ctype,
                          CASE
                            WHEN ctype = 'buildings'
                              THEN sum(CASE
                                         WHEN nri_category IN ('coastal')
                                           THEN hlr * CFLD_EXPB * CFLD_AFREQ
                                         WHEN nri_category IN ('coldwave')
                                           THEN hlr * CWAV_EXPB * CWAV_AFREQ
                                         WHEN nri_category IN ('hurricane')
                                           THEN hlr * HRCN_EXPB * HRCN_AFREQ
                                         WHEN nri_category IN ('heatwave')
                                           THEN hlr * HWAV_EXPB * HWAV_AFREQ
                                         WHEN nri_category IN ('hail')
                                           THEN hlr * HAIL_EXPB * HAIL_AFREQ
                                         WHEN nri_category IN ('tornado')
                                           THEN hlr * TRND_EXPB * TRND_AFREQ
                                         WHEN nri_category IN ('riverine')
                                           THEN hlr * RFLD_EXPB * RFLD_AFREQ
                                         WHEN nri_category IN ('lightning')
                                           THEN hlr * LTNG_EXPB * LTNG_AFREQ
                                         WHEN nri_category IN ('landslide')
                                           THEN hlr * LNDS_EXPB * LNDS_AFREQ
                                         WHEN nri_category IN ('icestorm')
                                           THEN hlr * ISTM_EXPB * ISTM_AFREQ
                                         WHEN nri_category IN ('wind')
                                           THEN hlr * SWND_EXPB * SWND_AFREQ
                                         WHEN nri_category IN ('wildfire')
                                           THEN (
                                                  CASE WHEN geoid IN (select stcofips
                                                                      FROM ${nri_schema}.${nri_table}
                                                                      where wfir_hlrb = 0.4)
                                                         THEN 0.4
                                                       ELSE hlr
                                                    END
                                                  ) * WFIR_EXPB * WFIR_AFREQ
                                         WHEN nri_category IN ('winterweat')
                                           THEN hlr * WNTW_EXPB * WNTW_AFREQ
                                         WHEN nri_category IN ('tsunami')
                                           THEN hlr * TSUN_EXPB * TSUN_AFREQ
                                         WHEN nri_category IN ('avalanche')
                                           THEN hlr * AVLN_EXPB * AVLN_AFREQ
                                         WHEN nri_category IN ('volcano')
                                           THEN hlr * VLCN_EXPB * VLCN_AFREQ
                              END)
                            WHEN ctype = 'crop'
                              THEN sum(CASE
                                         WHEN nri_category IN ('coldwave')
                                           THEN hlr * CWAV_EXPA * CWAV_AFREQ
                                         WHEN nri_category IN ('drought')
                                           THEN hlr * DRGT_EXPA * DRGT_AFREQ
                                         WHEN nri_category IN ('hurricane')
                                           THEN hlr * HRCN_EXPA * HRCN_AFREQ
                                         WHEN nri_category IN ('heatwave')
                                           THEN hlr * HWAV_EXPA * HWAV_AFREQ
                                         WHEN nri_category IN ('hail')
                                           THEN hlr * HAIL_EXPA * HAIL_AFREQ
                                         WHEN nri_category IN ('tornado')
                                           THEN hlr * TRND_EXPA * TRND_AFREQ
                                         WHEN nri_category IN ('riverine')
                                           THEN hlr * RFLD_EXPA * RFLD_AFREQ
                                         WHEN nri_category IN ('wind')
                                           THEN hlr * SWND_EXPA * SWND_AFREQ
                                         WHEN nri_category IN ('wildfire')
                                           THEN hlr * WFIR_EXPA * WFIR_AFREQ
                                         WHEN nri_category IN ('winterweat')
                                           THEN hlr * WNTW_EXPA * WNTW_AFREQ
                              END)
                            WHEN ctype = 'population'
                              THEN sum(CASE
                                         WHEN nri_category IN ('coastal')
                                           THEN hlr * CFLD_EXPPE * CFLD_AFREQ
                                         WHEN nri_category IN ('coldwave')
                                           THEN hlr * CWAV_EXPPE * CWAV_AFREQ
                                         WHEN nri_category IN ('hurricane')
                                           THEN hlr * HRCN_EXPPE * HRCN_AFREQ
                                         WHEN nri_category IN ('heatwave')
                                           THEN hlr * HWAV_EXPPE * HWAV_AFREQ
                                         WHEN nri_category IN ('hail')
                                           THEN hlr * HAIL_EXPPE * HAIL_AFREQ
                                         WHEN nri_category IN ('tornado')
                                           THEN hlr * TRND_EXPPE * TRND_AFREQ
                                         WHEN nri_category IN ('riverine')
                                           THEN hlr * RFLD_EXPPE * RFLD_AFREQ
                                         WHEN nri_category IN ('lightning')
                                           THEN hlr * LTNG_EXPPE * LTNG_AFREQ
                                         WHEN nri_category IN ('landslide')
                                           THEN hlr * LNDS_EXPPE * LNDS_AFREQ
                                         WHEN nri_category IN ('icestorm')
                                           THEN hlr * ISTM_EXPPE * ISTM_AFREQ
                                         WHEN nri_category IN ('wind')
                                           THEN hlr * SWND_EXPPE * SWND_AFREQ
                                         WHEN nri_category IN ('wildfire')
                                           THEN hlr * WFIR_EXPPE * WFIR_AFREQ
                                         WHEN nri_category IN ('winterweat')
                                           THEN hlr * WNTW_EXPPE * WNTW_AFREQ
                                         WHEN nri_category IN ('tsunami')
                                           THEN hlr * TSUN_EXPPE * TSUN_AFREQ
                                         WHEN nri_category IN ('avalanche')
                                           THEN hlr * AVLN_EXPPE * AVLN_AFREQ
                                         WHEN nri_category IN ('volcano')
                                           THEN hlr * VLCN_EXPPE * VLCN_AFREQ
                              END)
                            END avail
                   FROM ${hlr_schema}.${hlr_table}
                          JOIN ${nri_schema}.${nri_table}
                               ON geoid = stcofips
                   GROUP BY 1, 2

                   ORDER BY 1, 2)


  select nri_category,
         sum(case when ctype = 'buildings' then avail else 0 end)  swd_buildings,
         sum(case when ctype = 'crop' then avail else 0 end)       swd_crop,
         sum(case when ctype = 'population' then avail else 0 end) swd_population
  INTO ${hlr_schema}.${table_name}_${view_id}
  from dollars
  group by 1
  order by 1;
`;
