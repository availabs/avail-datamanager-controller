with per_basis_summary as (
  SELECT nri_category, count(1),
         sum(
           case
             when coalesce(swd_property_damage,0) + coalesce(swd_crop_damage,0) + coalesce(fatalities_dollar_value) > 0 then 1
             else 0
             end
           ) as swd_events,
         sum(
           case
             when coalesce(fema_property_damage,0) + coalesce(fema_crop_damage,0) > 0 then 1
             else 0
             end
           ) as fema_events,
         sum(swd_property_damage) as per_basis_property,
         sum(swd_crop_damage) as per_basis_crop
  FROM public.tmp_per_basis_data_zero_loss
  group by 1
),
     swd_summary as (
       SELECT nri_category as nri_category, count(1),
              sum(
                case
                  when coalesce(property_damage,0) + coalesce(crop_damage,0) + coalesce(injuries_direct,0) + coalesce(injuries_indirect,0) + coalesce(deaths_direct,0) + coalesce(deaths_indirect,0) > 0 then 1
                  else 0
                  end
                ) as swd_events,

              sum(property_damage) as property_damage,
              sum(crop_damage) as crop_damage,
              coalesce(sum(
                           coalesce(deaths_direct::float,0) +
                           coalesce(deaths_indirect::float,0) +
                           (
                               (
                                   coalesce(injuries_direct::float,0) +
                                   coalesce(injuries_indirect::float,0)
                                 ) / 10
                             )
                         ), 0) * 7600000   as person_damage
       FROM severe_weather_new.details
       where (year >= 1996 and year <=2019)
         and nri_category not in ('OTHER','Marine Dense Fog', 'Dense Fog', 'Astronomical Low Tide','Dust Devil','Dense Smoke','Dust Storm', 'Northern Lights')
       group by 1
     ),
     swd_summary_2 as (
       SELECT nri_category as nri_category, count(1),
              sum(
                case
                  when coalesce(property_damage,0) + coalesce(crop_damage,0) + coalesce(injuries_direct,0) + coalesce(injuries_indirect,0) + coalesce(deaths_direct,0) + coalesce(deaths_indirect,0) > 0 then 1
                  else 0
                  end
                ) as swd_events,
              sum(property_damage) as property_damage,
              sum(crop_damage) as crop_damage,
              coalesce(sum(
                           coalesce(deaths_direct::float,0) +
                           coalesce(deaths_indirect::float,0) +
                           (
                               (
                                   coalesce(injuries_direct::float,0) +
                                   coalesce(injuries_indirect::float,0)
                                 ) / 10
                             )
                         ), 0) * 7600000   as person_damage
       FROM severe_weather_new.details
       where (year >= 1996 and year <=2019)
         and nri_category not in ('OTHER','Marine Dense Fog', 'Dense Fog', 'Astronomical Low Tide','Dust Devil','Dense Smoke','Dust Storm', 'Northern Lights')
       group by 1
     ),
     nri_summary as (
       select
         unnest(array['coastal', 'coldwave', 'drought','earthquake','hurricane',  'heatwave', 'hail','tornado', 'riverine', 'lightning','landslide',  'icestorm',
                'wind', 'wildfire',  'winterweat', 'tsunami','avalanche',  'volcano'
                  ]) as nri_category,
         unnest(array[
                  sum(CFLD_hlrb * CFLD_EXPB  * CFLD_AFREQ), sum(CWAV_hlrb * CWAV_EXPB  * CWAV_AFREQ), sum(HRCN_hlrb * HRCN_EXPB  * HRCN_AFREQ),
                sum(HWAV_hlrb * HWAV_EXPB  * HWAV_AFREQ),
                sum(HAIL_hlrb * HAIL_EXPB  * HAIL_AFREQ), sum(TRND_hlrb * TRND_EXPB  * TRND_AFREQ), sum(RFLD_hlrb * RFLD_EXPB  * RFLD_AFREQ),
                sum(LTNG_hlrb * LTNG_EXPB  * LTNG_AFREQ), sum(LNDS_hlrb * LNDS_EXPB  * LNDS_AFREQ), sum(ISTM_hlrb * ISTM_EXPB  * ISTM_AFREQ),
                sum(SWND_hlrb * SWND_EXPB  * SWND_AFREQ),
                sum(WFIR_hlrb * WFIR_EXPB  * WFIR_AFREQ), sum(WNTW_hlrb * WNTW_EXPB  * WNTW_AFREQ), sum(TSUN_hlrb * TSUN_EXPB  * TSUN_AFREQ),
                sum(AVLN_hlrb * AVLN_EXPB  * AVLN_AFREQ), sum(VLCN_hlrb * VLCN_EXPB  * VLCN_AFREQ)
                  ]) as nri_property
       FROM national_risk_index.nri_counties_november_2021 nri
     ),
     hlr_summary as (
       SELECT nri_category,
              sum(CASE
                    WHEN nri_category IN ('coastal')
                      THEN hlr_b * CFLD_EXPB  * CFLD_AFREQ
                    WHEN nri_category IN ('coldwave')
                      THEN hlr_b * CWAV_EXPB  * CWAV_AFREQ
                    WHEN nri_category IN ('drought')
                      THEN hlr_b * CWAV_EXPB  * CWAV_AFREQ
                    WHEN nri_category IN ('hurricane')
                      THEN hlr_b * HRCN_EXPB  * HRCN_AFREQ
                    WHEN nri_category IN ('heatwave')
                      THEN hlr_b * HWAV_EXPB  * HWAV_AFREQ
                    WHEN nri_category IN ('hail')
                      THEN hlr_b * HAIL_EXPB  * HAIL_AFREQ
                    WHEN nri_category IN ('tornado')
                      THEN hlr_b * TRND_EXPB  * TRND_AFREQ
                    WHEN nri_category IN ('riverine')
                      THEN hlr_b * RFLD_EXPB  * RFLD_AFREQ
                    WHEN nri_category IN ('lightning')
                      THEN hlr_b * LTNG_EXPB  * LTNG_AFREQ
                    WHEN nri_category IN ('landslide')
                      THEN hlr_b * LNDS_EXPB  * LNDS_AFREQ
                    WHEN nri_category IN ('icestorm')
                      THEN hlr_b * ISTM_EXPB  * ISTM_AFREQ
                    WHEN nri_category IN ('wind')
                      THEN hlr_b * SWND_EXPB  * SWND_AFREQ
                    WHEN nri_category IN ('wildfire')
                      THEN hlr_b * WFIR_EXPB  * WFIR_AFREQ
                    WHEN nri_category IN ('winterweat')
                      THEN hlr_b * WNTW_EXPB  * WNTW_AFREQ
                    WHEN nri_category IN ('tsunami')
                      THEN hlr_b * TSUN_EXPB  * TSUN_AFREQ
                    WHEN nri_category IN ('avalanche')
                      THEN hlr_b * AVLN_EXPB  * AVLN_AFREQ
                    WHEN nri_category IN ('volcano')
                      THEN hlr_b * VLCN_EXPB  * VLCN_AFREQ
                END) swd_building,



              sum(CASE
                    WHEN nri_category IN ('coldwave')
                      THEN hlr_c * CWAV_EXPA  * CWAV_AFREQ
                    WHEN nri_category IN ('drought')
                      THEN hlr_c * CWAV_EXPA  * CWAV_AFREQ
                    WHEN nri_category IN ('hurricane')
                      THEN hlr_c * HRCN_EXPA  * HRCN_AFREQ
                    WHEN nri_category IN ('heatwave')
                      THEN hlr_c * HWAV_EXPA  * HWAV_AFREQ
                    WHEN nri_category IN ('hail')
                      THEN hlr_c * HAIL_EXPA  * HAIL_AFREQ
                    WHEN nri_category IN ('tornado')
                      THEN hlr_c * TRND_EXPA  * TRND_AFREQ
                    WHEN nri_category IN ('riverine')
                      THEN hlr_c * RFLD_EXPA  * RFLD_AFREQ
                    WHEN nri_category IN ('wind')
                      THEN hlr_c * SWND_EXPA  * SWND_AFREQ
                    WHEN nri_category IN ('wildfire')
                      THEN hlr_c * WFIR_EXPA  * WFIR_AFREQ
                    WHEN nri_category IN ('winterweat')
                      THEN hlr_c * WNTW_EXPA  * WNTW_AFREQ
                END) swd_crop,




              sum(CASE
                    WHEN nri_category IN ('coastal')
                      THEN hlr_p * CFLD_EXPPE  * CFLD_AFREQ
                    WHEN nri_category IN ('coldwave')
                      THEN hlr_p * CWAV_EXPPE  * CWAV_AFREQ
                    WHEN nri_category IN ('drought')
                      THEN hlr_p * CWAV_EXPPE  * CWAV_AFREQ
                    WHEN nri_category IN ('hurricane')
                      THEN hlr_p * HRCN_EXPPE  * HRCN_AFREQ
                    WHEN nri_category IN ('heatwave')
                      THEN hlr_p * HWAV_EXPPE  * HWAV_AFREQ
                    WHEN nri_category IN ('hail')
                      THEN hlr_p * HAIL_EXPPE  * HAIL_AFREQ
                    WHEN nri_category IN ('tornado')
                      THEN hlr_p * TRND_EXPPE  * TRND_AFREQ
                    WHEN nri_category IN ('riverine')
                      THEN hlr_p * RFLD_EXPPE  * RFLD_AFREQ
                    WHEN nri_category IN ('lightning')
                      THEN hlr_p * LTNG_EXPPE  * LTNG_AFREQ
                    WHEN nri_category IN ('landslide')
                      THEN hlr_p * LNDS_EXPPE  * LNDS_AFREQ
                    WHEN nri_category IN ('icestorm')
                      THEN hlr_p * ISTM_EXPPE  * ISTM_AFREQ
                    WHEN nri_category IN ('wind')
                      THEN hlr_p * SWND_EXPPE  * SWND_AFREQ
                    WHEN nri_category IN ('wildfire')
                      THEN hlr_p * WFIR_EXPPE  * WFIR_AFREQ
                    WHEN nri_category IN ('winterweat')
                      THEN hlr_p * WNTW_EXPPE  * WNTW_AFREQ
                    WHEN nri_category IN ('tsunami')
                      THEN hlr_p * TSUN_EXPPE  * TSUN_AFREQ
                    WHEN nri_category IN ('avalanche')
                      THEN hlr_p * AVLN_EXPPE  * AVLN_AFREQ
                    WHEN nri_category IN ('volcano')
                      THEN hlr_p * VLCN_EXPPE  * VLCN_AFREQ
                END) swd_people

       FROM public.tmp_hlr_v1
              JOIN national_risk_index.nri_counties_november_2021
                   ON geoid = stcofips
       GROUP BY nri_category

       ORDER BY nri_category
     ),nri_doc_counts as (
  select
    unnest(array['avalanche',
           'coastal',
           'coldwave',
           'drought',
           'earthquake',
           'hail',
           'heatwave',
           'hurricane',
           'icestorm',
           'landslide',
           'lightning',
           'riverine',
           'tornado',
           'tsunami',
           'volcano',
           'wildfire',
           'winds',
           'winterweat']) as nri_category,
    unnest(array[1344,
           818,
           2857,
           5232,
           212,
           27552,
           2611,
           4708,
           3888,
           852,
           14439,
           52103,
           17710,
           28,
           42,
           3083,
           200255,
           15154]) as loss_records,
    unnest(array[
             1174,
           643,
           3696,
           145001,
           207,
           18719,
           11781,
           3672,
           6671,
           649,
           13232,
           118208,
           17675,
           23,
           31,
           2367,
           148723,
           25830]) as per_basis_loss_records
)
-- select * from hlr_summary

select a.nri_category,
       a.swd_events,
       f.loss_records,
       ((per_basis_property-a.property_damage) / nullif(a.property_damage,0)) * 100 as property_damage_ratio,
       ((per_basis_crop - a.crop_damage) / nullif(a.crop_damage,0)) * 100 as crop_damage_ratio,
       round(((property_damage / 24)/1000000000),3) as swd_annual,
       --per_basis_property / 24 as per_basis_year,
       round(((d.swd_building/1000000000)::numeric),3) as hlr_annual,
       round(((c.nri_property/1000000000)::numeric),3) as nri_annual,
       ((( (property_damage / 24) - nri_property) / nullif((c.nri_property),0)) * 100)::int as nri_swd_ratio,
  ((( (d.swd_building) - nri_property) / nullif((c.nri_property),0)) * 100)::int as nri_hlr_ratio
from swd_summary as a
       join nri_doc_counts as f on a.nri_category = f.nri_category
       join per_basis_summary as b on a.nri_category = b.nri_category
       join nri_summary  as c on a.nri_category = c.nri_category
       join hlr_summary as d on a.nri_category = d.nri_category
order by nri_category asc



--   hazard,
-- # swd events,
-- # 0 loss swd events,
-- # loss swd events,
-- # pb events,
-- # 0 loss pb events,
-- # loss pb events,
-- $ swd loss total
-- $ pb loss total
-- $ swd loss annual
-- $ avail_eal
-- $ nri_eal

-- enhanced ncei
-- pb table
-- avail hlr table
-- nri table
