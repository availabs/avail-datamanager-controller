export const ofd = ({
                      table_name, ofd_schema, view_id,
                      pafpd_table, ihp_table, dds_table, sba_table,
                      nfip_table, usda_table,
                    }
) => `
with pa_data as (
    select disaster_number::text,
           lpad(state_number_code::text, 2, '0') || lpad(county_code::text, 3, '0') geoid,
           extract(YEAR from declaration_date) as year,
           incident_type,
           sum(coalesce(project_amount, 0)) project_amount
    from ${ofd_schema}.${pafpd_table}
    where dcc not in ('A', 'B', 'Z')
      and lpad(state_number_code::text, 2, '0') || lpad(county_code::text, 3, '0') not like '*'
      and extract(YEAR from declaration_date) >= 1996 and extract(YEAR from declaration_date) <= 2019
      and not (disaster_number >= 3000 and disaster_number <= 3999)
    group by 1, 2, 3, 4
),
     ihp as (
         SELECT substring(ihp.geoid, 1, 5) geoid,
                ihp.disaster_number,
                extract(YEAR from ihp.declaration_date) as year,
                ihp.incident_type,
                sum(coalesce(rpfvl, 0) + coalesce(ppfvl, 0))                                as ihp_verified_loss,
                sum(coalesce(ha_amount, 0))    as ha_loss
         FROM ${ofd_schema}.${ihp_table} ihp
         where extract(YEAR from ihp.declaration_date) >= 1996 and extract(YEAR from ihp.declaration_date) <= 2019
           and substring(ihp.geoid, 1, 5) not like '*'
           and not (disaster_number::numeric >= 3000 and disaster_number::numeric <= 3999)
         group by 1,2,3,4
     ),
     ihp_pa_merge as (
         SELECT coalesce(ihp.geoid, pa.geoid) as geoid,
                coalesce(ihp.disaster_number, pa.disaster_number) as disaster_number,
                coalesce(ihp.year, pa.year) as year,
                coalesce(ihp.incident_type, pa.incident_type) as incident_type,

                sum(coalesce(ihp_verified_loss, 0))                                   as ihp_verified_loss,
                sum(coalesce(ha_loss, 0))    as ha_loss,
                sum(coalesce(project_amount,0))    as project_amount
         FROM ihp
                  FULL OUTER JOIN pa_data pa
                                  ON ihp.disaster_number = pa.disaster_number
                                      AND ihp.geoid = pa.geoid
         WHERE coalesce(ihp_verified_loss, 0) + coalesce(ha_loss, 0) + coalesce(project_amount, 0) > 0
         GROUP BY 1,2,3,4
     ),
     ofd as (
         SELECT
             dd.disaster_number::text,
             coalesce(dd.incident_type, ihp_pa_merge.incident_type) incident_type,
             fips_state_code || fips_county_code                           geoid,
             extract(YEAR from incident_begin_date)                        AS year,
             min(incident_begin_date)                                      fema_incident_begin_date,
             max(incident_end_date)                                        fema_incident_end_date,
             sum(ihp_verified_loss)                                        ihp_verified_loss,
             sum(ha_loss)      ha_loss,
             sum(project_amount)          project_amount
         FROM  ${ofd_schema}.${dds_table} dd
                   FULL OUTER JOIN ihp_pa_merge
                                   ON dd.disaster_number::text = ihp_pa_merge.disaster_number
                                       AND fips_state_code || fips_county_code = geoid
         WHERE coalesce(fy_declared, extract(YEAR from incident_begin_date)) >= 1996 and coalesce(fy_declared, extract(YEAR from incident_begin_date)) <= 2019
           AND dd.fips_state_code || dd.fips_county_code not like '*'
           AND dd.incident_type not in ('Biological', 'Other', 'Terrorist', 'Chemical', 'Human Cause', 'Toxic Substances',
                                        'Dense Fog','Dense Smoke','Dust Devil','Dust Storm'
             )
           and not (dd.disaster_number >= 3000 and dd.disaster_number <= 3999)
         GROUP BY 1, 2, 3, 4
         ORDER BY ihp_verified_loss, ha_loss, project_amount
     ),
     sba as (
         SELECT replace(fema_disaster_number, 'DR', '')                  disaster_number,
                incident_type,
                substring(geoid, 1, 5)                geoid,
                year,
                SUM(coalesce(total_verified_loss, 0)) sba_loss
         FROM ${ofd_schema}.${sba_table} sba
                  JOIN ${ofd_schema}.${dds_table} dd
                       ON dd.disaster_number::text = replace(fema_disaster_number, 'DR', '')
                           AND geoid = fips_state_code || fips_county_code
         WHERE year >= 1996 and year <= 2019
           AND substring(geoid, 1, 5) not like '*'
           and not (disaster_number::numeric >= 3000 and disaster_number::numeric <= 3999)
         GROUP BY 1, 2, 3, 4
     ),
     nfipPayout as (
         select year_of_loss, county_code, disaster_number::text, incident_type,
                coalesce(sum(amount_paid_on_contents_claim), 0) +
                coalesce(sum(amount_paid_on_building_claim), 0) +
                coalesce(sum(amount_paid_on_increased_cost_of_compliance_claim), 0) total_amount_paid

         FROM ${ofd_schema}.${nfip_table} nfip
                  JOIN ${ofd_schema}.${dds_table} dd
                       ON date_of_loss BETWEEN incident_begin_date AND incident_end_date
                           AND dd.incident_type IN ('Flood', 'Hurricane', 'Severe Storm(s)', 'Coastal Storm', 'Tornado', 'Dam/Levee Break', 'Typhoon')
                           AND county_code = fips_state_code || fips_county_code
         WHERE year_of_loss >= 1996 and year_of_loss <= 2019
           AND county_code not like '*'
           and not (dd.disaster_number >= 3000 and dd.disaster_number <= 3999)
         group by 1, 2, 3, 4
     ),
     disasters as (
         SELECT disaster_number,
                incident_type,
                fips_state_code || fips_county_code          county,
                EXTRACT(YEAR FROM MIN(incident_begin_date))  begin_year,
                EXTRACT(YEAR FROM MAX(incident_end_date))    end_year,
                EXTRACT(MONTH FROM MIN(incident_begin_date)) begin_month,
                EXTRACT(MONTH FROM MAX(incident_end_date))   end_month
         FROM ${ofd_schema}.${dds_table}
         WHERE EXTRACT(YEAR FROM incident_begin_date ) >= 1996 and EXTRACT(YEAR FROM incident_begin_date ) <= 2019
           AND fips_state_code || fips_county_code not like '*'
           and not (disaster_number >= 3000 and disaster_number <= 3999)
         GROUP BY 1, 2, 3
         ORDER BY 1 DESC
     ),
     crop_loss as (
         SELECT state_fips || county_fips geoid,
                commodity_year_identifier::int "year",
                month_of_loss::int "month",
                CASE
                    WHEN cause_of_loss_desc IN ('Excess Moisture/Precipitation/Rain','Flood','Poor Drainage')
                        THEN 'Flood'
                    WHEN cause_of_loss_desc IN ('Hail')
                        THEN 'Severe Storm(s)'
                    WHEN cause_of_loss_desc IN ('Storm Surge')
                        THEN 'Coastal Storm'
                    WHEN cause_of_loss_desc IN ('Hurricane/Tropical Depression')
                        THEN 'Hurricane'
                    WHEN cause_of_loss_desc IN ('Tidal Wave/Tsunami')
                        THEN 'Tsunami'
                    WHEN cause_of_loss_desc IN ('Tornado')
                        THEN 'Tornado'
                    WHEN cause_of_loss_desc IN ('Other (Snow,Lightning,etc)','Other (Snow,Lightning,etc)','Other (Snow, Lightning, Etc.)','Other (Volcano,Snow,Lightning,etc)','Other (Volcano,Snow,Lightning,etc)')
                        THEN 'Snow'
                    WHEN cause_of_loss_desc IN ('Freeze','Cold Winter','Cold Wet Weather','Frost','Ice Flow','Ice Floe')
                        THEN 'Freezing'
                    WHEN cause_of_loss_desc IN ('Cyclone')
                        THEN 'Typhoon'
                    WHEN cause_of_loss_desc IN ('Earthquake')
                        THEN 'Earthquake'
                    WHEN cause_of_loss_desc IN ('Volcanic Eruption')
                        THEN 'Volcano'
                    WHEN cause_of_loss_desc IN ('Force Fire','House burn (Pole burn)','Fire','Pit Burn','House Burn (Pole Burn)')
                        THEN 'Fire'
                    WHEN cause_of_loss_desc IN ('Drought','Drought Deviation')
                        THEN 'Drought'
                    ELSE cause_of_loss_desc
                    END cause_of_loss_desc,
                indemnity_amount
         FROM ${ofd_schema}.${usda_table}
         WHERE cause_of_loss_desc in (
                                      'Excess Moisture/Precipitation/Rain','Flood','Poor Drainage','Hail','Storm Surge',
                                      'Hurricane/Tropical Depression','Tidal Wave/Tsunami','Tornado','Other (Snow,Lightning,etc)',
                                      'Other (Snow,Lightning,etc)','Other (Snow, Lightning, Etc.)','Other (Volcano,Snow,Lightning,etc)',
                                      'Other (Volcano,Snow,Lightning,etc)','Freeze','Cold Winter','Cold Wet Weather','Frost','Ice Flow',
                                      'Ice Floe','Cyclone','Earthquake','Volcanic Eruption','Force Fire','House burn (Pole burn)','Fire',
                                      'Pit Burn','House Burn (Pole Burn)','Drought','Drought Deviation'
             )
           AND state_fips || county_fips not like '*'
           AND month_of_loss != ''
           AND commodity_year_identifier::int >= 1996
           AND commodity_year_identifier::int <= 2019
     ),
     croploss as (
         SELECT geoid, disaster_number::text, incident_type, year, SUM(indemnity_amount) crop_loss
         FROM disasters d
                  JOIN crop_loss c
                       ON d.county = c.geoid
                           AND (c.year BETWEEN d.begin_year AND d.end_year OR c.year = d.begin_year)
                           AND (c.month BETWEEN d.begin_month AND d.end_month OR c.month = d.begin_month)
                           AND d.incident_type = c.cause_of_loss_desc
         WHERE geoid not like '*'
         GROUP BY 1, 2, 3, 4
         ORDER BY 1, 2
     ),
     disaster_declarations_summary as (
         SELECT
             coalesce(coalesce(coalesce(ofd.disaster_number, sba.disaster_number),
                               nfip.disaster_number), croploss.disaster_number)                                            disaster_number,
             CASE
                 WHEN lower(coalesce(coalesce(coalesce(ofd.incident_type, sba.incident_type),
                                              nfip.incident_type), croploss.incident_type)) = 'coastal storm'
                     THEN 'coastal'
                 WHEN lower(coalesce(coalesce(coalesce(ofd.incident_type, sba.incident_type),
                                              nfip.incident_type), croploss.incident_type)) IN ('dam/levee break', 'flood', 'severe storm', 'severe storm(s)', 'heavy rain')
                     THEN 'riverine'
                 WHEN lower(coalesce(coalesce(coalesce(ofd.incident_type, sba.incident_type),
                                              nfip.incident_type), croploss.incident_type)) = 'drought'
                     THEN 'drought'
                 WHEN lower(coalesce(coalesce(coalesce(ofd.incident_type, sba.incident_type),
                                              nfip.incident_type), croploss.incident_type)) = 'fire'
                     THEN 'wildfire'
                 WHEN lower(coalesce(coalesce(coalesce(ofd.incident_type, sba.incident_type),
                                              nfip.incident_type), croploss.incident_type)) = 'freezing'
                     THEN 'coldwave'
                 WHEN lower(coalesce(coalesce(coalesce(ofd.incident_type, sba.incident_type),
                                              nfip.incident_type), croploss.incident_type)) IN ('hurricane', 'typhoon')
                     THEN 'hurricane'
                 WHEN lower(coalesce(coalesce(coalesce(ofd.incident_type, sba.incident_type),
                                              nfip.incident_type), croploss.incident_type)) = 'mud/landslide'
                     THEN 'landslide'
                 WHEN lower(coalesce(coalesce(coalesce(ofd.incident_type, sba.incident_type),
                                              nfip.incident_type), croploss.incident_type)) = 'severe ice storm'
                     THEN 'icestorm'
                 WHEN lower(coalesce(coalesce(coalesce(ofd.incident_type, sba.incident_type),
                                              nfip.incident_type), croploss.incident_type)) IN ('snow', 'freezing fog')
                     THEN 'winterweat'
                 WHEN lower(coalesce(coalesce(coalesce(ofd.incident_type, sba.incident_type),
                                              nfip.incident_type), croploss.incident_type)) = 'earthquake'
                     THEN 'earthquake'
                 WHEN lower(coalesce(coalesce(coalesce(ofd.incident_type, sba.incident_type),
                                              nfip.incident_type), croploss.incident_type)) = 'tornado'
                     THEN 'tornado'
                 WHEN lower(coalesce(coalesce(coalesce(ofd.incident_type, sba.incident_type),
                                              nfip.incident_type), croploss.incident_type)) = 'tsunami'
                     THEN 'tsunami'
                 WHEN lower(coalesce(coalesce(coalesce(ofd.incident_type, sba.incident_type),
                                              nfip.incident_type), croploss.incident_type)) = 'volcano'
                     THEN 'volcano'
                 END incident_type,
             coalesce(coalesce(coalesce(ofd.geoid, sba.geoid), nfip.county_code), croploss.geoid)        geoid,
             min(ofd.fema_incident_begin_date)                                                           fema_incident_begin_date,
             max(ofd.fema_incident_end_date)                                                             fema_incident_end_date,
             sum(coalesce(ihp_verified_loss, 0) + coalesce(project_amount, 0) +
                 coalesce(sba_loss, 0) + coalesce(total_amount_paid, 0))                                 fema_property_damage,
             sum(coalesce(ihp_verified_loss, 0)) ihp_loss,
             sum(coalesce(project_amount, 0)) pa_loass,
             sum(coalesce(sba_loss, 0)) sba_loss,
             sum(coalesce(total_amount_paid, 0)) nfip_loss,
             sum(coalesce(crop_loss, 0))                                                                 fema_crop_damage

         FROM ofd
                  FULL OUTER JOIN sba
                                  ON ofd.disaster_number = sba.disaster_number
                                      AND ofd.geoid = sba.geoid
                                      AND ofd.year = sba.year

                  FULL OUTER JOIN nfipPayout nfip
                                  ON ofd.geoid = nfip.county_code
                                      AND ofd.disaster_number = nfip.disaster_number

                  FULL OUTER JOIN croploss
                                  ON ofd.disaster_number = croploss.disaster_number
                                      AND ofd.geoid = croploss.geoid
                                      AND ofd.year = croploss.year
         WHERE (
                           ihp_verified_loss > 0 OR project_amount > 0 OR sba_loss > 0 OR total_amount_paid > 0  OR crop_loss > 0
                   )

         GROUP BY 1, 2, 3
     )

select * into  ${ofd_schema}.${table_name}_${view_id} from disaster_declarations_summary


`;
