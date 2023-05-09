export const fusion = ({
                      table_name, ofd_schema, view_id,
                      dl_table, nceie_schema, nceie_table
                    }
) => `

  with disaster_declarations_summary_grouped_for_merge as (SELECT disaster_number,
                                                                  incident_type,
                                                                  ARRAY_AGG(geoid)                    counties,
                                                                  min(fema_incident_begin_date::date) fema_incident_begin_date,
                                                                  max(fema_incident_end_date::date)   fema_incident_end_date
                                                           FROM ${ofd_schema}.${dl_table}
                                                           GROUP BY 1, 2),
       disaster_number_to_event_id_mapping_without_hazard_type as (SELECT distinct d.disaster_number, event_id
                                                                   FROM ${nceie_schema}.${nceie_table} sw
                                                                          JOIN disaster_declarations_summary_grouped_for_merge d
                                                                               ON substring(geoid, 1, 5) = any
                                                                                  (d.counties)
                                                                                 AND (begin_date_time::date,
                                                                                      end_date_time::date) OVERLAPS
                                                                                     (fema_incident_begin_date,
                                                                                      fema_incident_end_date)
                                                                                 AND (
                                                                                        incident_type = nri_category OR
                                                                                        (incident_type = 'hurricane' AND nri_category = 'coastal') OR
                                                                                        (incident_type = 'hurricane' AND nri_category = 'wind') OR
                                                                                        (incident_type = 'hurricane' AND nri_category = 'riverine') OR
                                                                                        (incident_type = 'icestorm' AND nri_category = 'coldwave') OR
                                                                                        (incident_type = 'winterweat' AND nri_category = 'coldwave') OR
                                                                                        (incident_type = 'winterweat' AND nri_category = 'icestorm') OR
                                                                                        (incident_type = 'earthquake' AND nri_category = 'landslide') OR
                                                                                        (incident_type = 'tornado' AND nri_category = 'wind') OR
                                                                                        (incident_type = 'icestorm' AND nri_category = 'winterweat')
                                                                                    )
                                                                   WHERE year >= 1996 and year <= 2019
    AND nri_category not in ('Dense Fog', 'Marine Dense Fog', 'Dense Smoke', 'Dust Devil', 'Dust Storm', 'Astronomical Low Tide', 'Northern Lights', 'OTHER')
    AND geoid is not null
  ORDER BY d.disaster_number
    ),
    event_division_factor as (
  select event_id, count (1) division_factor
  from disaster_number_to_event_id_mapping_without_hazard_type
  group by event_id
  order by 1 desc
    ),
    swd as (
  SELECT
    substring (sw.geoid, 1, 5) geoid,
      sw.event_id,
      dn_eid.disaster_number,
      sw.nri_category nri_category,
      min (begin_date_time:: date) swd_begin_date,
      max (end_date_time:: date) swd_end_date,
      sum (property_damage)/ coalesce (edf.division_factor, 1) as swd_property_damage,
      sum (crop_damage)/ coalesce (edf.division_factor, 1) as swd_crop_damage,
      (
        sum (
                coalesce (deaths_direct:: float, 0) + coalesce (deaths_indirect:: float, 0) +
                ((coalesce (injuries_direct:: float, 0) + coalesce (injuries_indirect:: float, 0)) / 10)
            ) * 7600000)/ coalesce (edf.division_factor, 1) as swd_population_damage,
    coalesce (sum(deaths_direct):: float, 0) / coalesce (edf.division_factor, 1) as deaths_direct,
    coalesce (sum(deaths_indirect):: float, 0) / coalesce (edf.division_factor, 1) as deaths_indirect,
    coalesce (sum(injuries_direct):: float, 0) / coalesce (edf.division_factor, 1) as injuries_direct,
    coalesce (sum(injuries_indirect):: float, 0) / coalesce (edf.division_factor, 1) as injuries_indirect
  FROM ${nceie_schema}.${nceie_table} sw
    LEFT JOIN disaster_number_to_event_id_mapping_without_hazard_type dn_eid
  on sw.event_id = dn_eid.event_id
    LEFT JOIN event_division_factor edf
    ON edf.event_id = sw.event_id
  WHERE year >= 1996
    and year <= 2019
    AND nri_category not in ('Dense Fog', 'Marine Dense Fog', 'Dense Smoke', 'Dust Devil', 'Dust Storm', 'Astronomical Low Tide', 'Northern Lights', 'OTHER')
    AND (sw.geoid is not null)
  group by 1, 2, 3, 4, division_factor
  order by 1, 2, 3, 4
    ),
    full_data as (
  SELECT coalesce (swd.geoid, ofd.geoid) geoid,
      event_id,
      ofd.disaster_number,
      coalesce (nri_category, incident_type) nri_category,
    incident_type fema_incident_type,
      swd_begin_date, swd_end_date,
      fema_incident_begin_date,
      fema_incident_end_date,
      coalesce(fema_property_damage, 0) fema_property_damage,
      coalesce(fema_crop_damage, 0) fema_crop_damage,

      coalesce(swd_property_damage, 0) swd_property_damage,
      coalesce(swd_crop_damage, 0) swd_crop_damage,
      coalesce(swd_population_damage, 0) swd_population_damage,
      coalesce ((deaths_direct):: float, 0) as deaths_direct,
      coalesce ((deaths_indirect):: float, 0) as deaths_indirect,
      coalesce ((injuries_direct):: float, 0) as injuries_direct,
      coalesce ((injuries_indirect):: float, 0) as injuries_indirect,
    coalesce(swd_property_damage, 0) fusion_property_damage,
    coalesce(swd_crop_damage, 0) fusion_crop_damage

  FROM swd
  FULL OUTER JOIN ${ofd_schema}.${dl_table} ofd
  ON swd.disaster_number = ofd.disaster_number
    AND swd.geoid = ofd.geoid
    ),



    disaster_division_factor as (
      select disaster_number, geoid, count (1) ddf
      from full_data
      group by 1, 2
      order by 1, 2
    ),

    full_adjusted as (
  SELECT fd.geoid,
      event_id,
      fd.disaster_number,
      nri_category,
    fema_incident_type,
    swd_begin_date,
    swd_end_date,
    fema_incident_begin_date,
    fema_incident_end_date,
    fema_property_damage/ coalesce (ddf, 1) fema_property_damage,
    fema_crop_damage/ coalesce (ddf, 1) fema_crop_damage,
    swd_property_damage, swd_crop_damage, swd_population_damage,
      deaths_direct, deaths_indirect, injuries_direct, injuries_indirect,
    fusion_property_damage,
    fusion_crop_damage
  FROM full_data fd
    LEFT JOIN disaster_division_factor ddf
  ON ddf.disaster_number = fd.disaster_number
    AND ddf.geoid = fd.geoid
    ),
    disaster_mapping_summary as (
  select disaster_number, geoid, fema_incident_type,

      min(fema_incident_begin_date) fema_incident_begin_date,
      max(fema_incident_end_date)   fema_incident_end_date,

      sum(fema_property_damage)     fema_property_damage,
      sum(fema_crop_damage)         fema_crop_damage,
      sum(swd_property_damage)      swd_property_damage,
      sum(swd_crop_damage)          swd_crop_damage
  from full_adjusted
  group by 1, 2, 3
  having sum(fema_property_damage) > sum(swd_property_damage) OR sum(fema_crop_damage) > sum(swd_crop_damage)
  order by 1, 2
    ),
    additional_rows as (
      SELECT
          geoid,
          null::integer as event_id,
          disaster_number,
          fema_incident_type as nri_category,
          fema_incident_type,
          null::date as swd_begin_date,
          null::date as swd_end_date,
          fema_incident_begin_date,
          fema_incident_end_date,

          0 as fema_property_damage,
          0 as fema_crop_damage,

          0 as swd_property_damage,
          0 as swd_crop_damage,
          0 as swd_population_damage,
          0 as deaths_direct,
          0 as deaths_indirect,
          0 as injuries_direct,
          0 as injuries_indirect,
          CASE WHEN fema_property_damage > swd_property_damage
               THEN fema_property_damage - swd_property_damage
               ELSE 0
          END fusion_property_damage,

          CASE WHEN fema_crop_damage > swd_crop_damage
               THEN fema_crop_damage - swd_crop_damage
               ELSE 0
          END fusion_crop_damage
      FROM disaster_mapping_summary
    )

select * into  ${ofd_schema}.${table_name}_${view_id}
         from (select * from full_adjusted union all select * from additional_rows) a
`;

