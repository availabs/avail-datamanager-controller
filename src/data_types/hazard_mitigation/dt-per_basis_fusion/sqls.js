export const per_basis_swd = (table_name, view_id, fusion_schema, fusion_table, startYear, endYear,) => `
  with   buildings as (
    select
      'buildings' ctype,
      event_id,
      substring(geoid, 1, 5) geoid,
      nri_category nri_category,
      min(coalesce(swd_begin_date, fema_incident_begin_date))::date     swd_begin_date,
      max(coalesce(swd_end_date, fema_incident_end_date))  ::date      swd_end_date,
      sum(fusion_property_damage)     damage
    FROM ${fusion_schema}.${fusion_table}
    WHERE extract(YEAR from coalesce(swd_begin_date, fema_incident_begin_date)) between ${startYear} and ${endYear}
      AND geoid is not null
      AND nri_category is not null
    GROUP BY 1, 2, 3, 4
),
       crop as (
           select
             'crop' ctype,
             event_id,
             substring(geoid, 1, 5) geoid,
             nri_category nri_category,
             min(coalesce(swd_begin_date, fema_incident_begin_date))::date      swd_begin_date,
             max(coalesce(swd_end_date, fema_incident_end_date))::date        swd_end_date,
             sum(fusion_crop_damage)         damage
           FROM ${fusion_schema}.${fusion_table}
           WHERE extract(YEAR from coalesce(swd_begin_date, fema_incident_begin_date)) between ${startYear} and ${endYear}
             AND geoid is not null
             AND nri_category is not null
           GROUP BY 1, 2, 3, 4
       ),
       population as (
           select
             'population' ctype,
             event_id,
             substring(geoid, 1, 5) geoid,
             nri_category nri_category,
             min(coalesce(swd_begin_date, fema_incident_begin_date))::date      swd_begin_date,
             max(coalesce(swd_end_date, fema_incident_end_date))::date        swd_end_date,
             sum(swd_population_damage)   damage
           FROM ${fusion_schema}.${fusion_table}
           WHERE extract(YEAR from coalesce(swd_begin_date, fema_incident_begin_date)) between ${startYear} and ${endYear}
             AND geoid is not null
             AND nri_category is not null
           GROUP BY 1, 2, 3, 4
       ), alldata as (
    select * from buildings

    union all

    select * from crop

    union all

    select * from population
),

       day_expansion as (
           SELECT ctype,
                  generate_series(swd_begin_date::date,
                                  LEAST(
                                          swd_end_date::date,
                                          CASE WHEN nri_category = 'drought' THEN swd_begin_date::date + INTERVAL '365 days' ELSE swd_begin_date::date + INTERVAL '31 days' END
                                      ), '1 day'::interval)::date event_day_date,
                  nri_category,
                  geoid,
                  sum(damage::double precision/LEAST(
                                  swd_end_date::date - swd_begin_date::date + 1,
                                  CASE WHEN nri_category = 'drought' THEN 365 ELSE 31 END)) damage
           FROM alldata
           WHERE nri_category in ('coldwave', 'drought', 'heatwave', 'icestorm', 'riverine', 'winterweat')
             AND geoid is not null
           group by 1, 2, 3, 4
           order by 1, 2, 3, 4),
       aggregation as (
           SELECT
               ctype,
               nri_category,
               geoid,
               swd_begin_date event_day_date,
               ARRAY_AGG(event_id)       event_ids,
               count(1)                  num_events,
               sum (damage) damage

           from alldata
           where nri_category NOT IN ('coldwave', 'drought', 'heatwave', 'icestorm', 'riverine', 'winterweat')
           group by  1, 2, 3, 4
       ),
       final as (
           select ctype, nri_category, geoid, event_day_date,
                  event_ids, num_events,
                  damage, null::double precision damage_adjusted
           from aggregation

           UNION ALL

           select ctype, nri_category, geoid, event_day_date,
                  null as event_ids, null as num_events,
                  damage, null::double precision damage_adjusted
           from day_expansion

           order by 1, 2, 3, 4
       )

SELECT row_number() over () id, * INTO ${fusion_schema}.${table_name}_${view_id} FROM final;
`;

export const pad_zero_losses = ({
                                  table_name,
                                  view_id,
                                  ncei_schema,
                                  ncei_table,
                                  fusion_schema,
                                  fusion_table,
                                  nri_schema,
                                  nri_table,
                                  startYear,
                                  endYear,
                                }) => `
with pb as (
    select pb.geoid geoid, pb.nri_category, ctype, event_day_date, tor_f_scale
    from ${fusion_schema}.${table_name}_${view_id} pb
             LEFT JOIN (
        SELECT DISTINCT substring(geoid, 1, 5) geoid,
                        nri_category cat,
                        begin_date_time::date,
                        (array_agg(tor_f_scale))[1] tor_f_scale
        FROM ${ncei_schema}.${ncei_table}
        WHERE nri_category = 'tornado'
        GROUP BY 1, 2, 3
    ) details
                       on pb.geoid = details.geoid
                           and event_day_date = begin_date_time
                           and pb.nri_category = details.cat
),
     zero_loss_count as (
         SELECT ctype, b.geoid, b.nri_category,
                (max(CASE
                       WHEN nri_category IN ('coldwave')
                         THEN CWAV_AFREQ
                         WHEN nri_category IN ('coastal')
                             THEN CFLD_AFREQ
                         WHEN nri_category IN ('drought')
                             THEN DRGT_AFREQ
                         WHEN nri_category IN ('hail')
                             THEN HAIL_AFREQ
                         WHEN nri_category IN ('heatwave')
                             THEN HWAV_AFREQ
                         WHEN nri_category IN ('hurricane')
                             THEN HRCN_AFREQ
                         WHEN nri_category IN ('icestorm')
                             THEN ISTM_AFREQ
                         WHEN nri_category IN ('lightning')
                             THEN LTNG_AFREQ
                         WHEN nri_category IN ('riverine')
                             THEN RFLD_AFREQ
                         WHEN nri_category IN ('wind')
                             THEN SWND_AFREQ
                         WHEN nri_category IN ('tsunami')
                             THEN TSUN_AFREQ
                         WHEN nri_category IN ('winterweat')
                             THEN WNTW_AFREQ
                         WHEN nri_category IN ('tornado')
                             THEN TRND_AFREQ
                         ELSE null
                    END) * (${endYear} - ${startYear})) - count(1) records_to_insert
         FROM ${nri_schema}.${nri_table} a
                  JOIN pb b
                       ON a.stcofips = b.geoid
         WHERE  nri_category IN
                (
                 'coldwave', 'coastal', 'drought', 'hail', 'heatwave',
                 'hurricane', 'icestorm', 'lightning', 'riverine',
                 'wind', 'tsunami', 'winterweat'
                    )
            OR (nri_category = 'tornado' and tor_f_scale not like '%4' and tor_f_scale not like '%5' )
         group by 1, 2, 3
     ),
     records_to_insert as (
         select generate_series(1, floor(records_to_insert)::integer),
                ctype, nri_category, geoid,
                null event_day_date, null event_ids, null num_events, 0 damage, 0 damage_adjusted
         from zero_loss_count
         where records_to_insert >= 1
           and nri_category not in ('drought')
           and ctype = 'buildings'

         union all

         select generate_series(1, floor(records_to_insert)::integer),
                ctype, nri_category, geoid,
                null event_day_date, null event_ids, null num_events, 0 damage, 0 damage_adjusted
         from zero_loss_count
         where records_to_insert >= 1
           and nri_category in (
                                'coldwave', 'drought', 'hail',
                                'heatwave', 'hurricane', 'riverine',
                                'wind', 'tornado', 'winterweat'
             )
           and ctype = 'crop'

         union all

         select generate_series(1, floor(records_to_insert)::integer),
                ctype, nri_category, geoid,
                null event_day_date, null event_ids, null num_events, 0 damage, 0 damage_adjusted
         from zero_loss_count
         where records_to_insert >= 1
           and nri_category not in ('drought')
           and ctype = 'population'
     )

INSERT INTO ${fusion_schema}.${table_name}_${view_id}
SELECT null id, ctype, nri_category, geoid, event_day_date::timestamp, event_ids::integer[], num_events::bigint, damage, damage_adjusted
FROM records_to_insert
`;

export const adjusted_dollar = (table_name, view_id, fusion_schema) => `
with cpi as (
  SELECT unnest(array[1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020]) as YEAR,
    unnest(array[154.4, 159.1, 161.6, 164.3, 168.8, 175.1, 177.1, 181.7, 185.2, 190.7, 198.3, 202.4, 211.1, 211.143, 216.687, 220.223, 226.655, 230.28, 233.916, 233.707, 236.916, 242.839, 247.867, 251.712, 257.971]) as JAN,
    unnest(array[154.9, 159.6, 161.9, 164.5, 169.8, 175.8, 177.8, 183.1, 186.2, 191.8, 198.7, 203.5, 211.7, 212.193, 216.741, 221.309, 227.663, 232.166, 234.781, 234.722, 237.111, 243.603, 248.991, 252.776, 258.678]) as FEB,
    unnest(array[155.7, 160, 162.2, 165, 171.2, 176.2, 178.8, 184.2, 187.4, 193.3, 199.8, 205.4, 213.5, 212.709, 217.631, 223.467, 229.392, 232.773, 236.293, 236.119, 238.132, 243.801, 249.554, 254.202, 258.115]) as Mar,
    unnest(array[156.3, 160.2, 162.5, 166.2, 171.3, 176.9, 179.8, 183.8, 188, 194.6, 201.5, 206.7, 214.8, 213.24, 218.009, 224.906, 230.085, 232.531, 237.072, 236.599, 239.261, 244.524, 250.546, 255.548, 256.389]) as Apr,
    unnest(array[156.6, 160.1, 162.8, 166.2, 171.5, 177.7, 179.8, 183.5, 189.1, 194.4, 202.5, 207.9, 216.6, 213.856, 218.178, 225.964, 229.815, 232.945, 237.9, 237.805, 240.236, 244.733, 251.588, 256.092, 256.394]) as May,
    unnest(array[156.7, 160.3, 163, 166.2, 172.4, 178, 179.9, 183.7, 189.7, 194.5, 202.9, 208.4, 218.8, 215.693, 217.965, 225.722, 229.478, 233.504, 238.343, 238.638, 241.038, 244.955, 251.989, 256.143, 257.797]) as Jun,
    unnest(array[157, 160.5, 163.2, 166.7, 172.8, 177.5, 180.1, 183.9, 189.4, 195.4, 203.5, 208.3, 219.964, 215.351, 218.011, 225.922, 229.104, 233.596, 238.25, 238.654, 240.647, 244.786, 252.006, 256.571, 259.101]) as Jul,
    unnest(array[157.3, 160.8, 163.4, 167.1, 172.8, 177.5, 180.7, 184.6, 189.5, 196.4, 203.9, 207.9, 219.086, 215.834, 218.312, 226.545, 230.379, 233.877, 237.852, 238.316, 240.853, 245.519, 252.146, 256.558, 259.918]) as Aug,
    unnest(array[157.8, 161.2, 163.6, 167.9, 173.7, 178.3, 181, 185.2, 189.9, 198.8, 202.9, 208.5, 218.783, 215.969, 218.439, 226.889, 231.407, 234.149, 238.031, 237.945, 241.428, 246.819, 252.439, 256.759, 260.28]) as Sep,
    unnest(array[158.3, 161.6, 164, 168.2, 174, 177.7, 181.3, 185, 190.9, 199.2, 201.8, 208.9, 216.573, 216.177, 218.711, 226.421, 231.317, 233.546, 237.433, 237.838, 241.729, 246.663, 252.885, 257.346, 260.388]) as Oct,
    unnest(array[158.6, 161.5, 164, 168.3, 174.1, 177.4, 181.3, 184.5, 191, 197.6, 201.5, 210.2, 212.425, 216.33, 218.803, 226.23, 230.221, 233.069, 236.151, 237.336, 241.353, 246.669, 252.038, 257.208, 260.229]) as Nov,
    unnest(array[158.6, 161.3, 163.9, 168.3, 174, 176.7, 180.9, 184.3, 190.3, 196.8, 201.8, 210, 210.228, 215.949, 219.179, 225.672, 229.601, 233.049, 234.812, 236.525, 241.432, 246.524, 251.233, 256.974, 260.474]) as Dec,
    unnest(array[156.9, 160.5, 163, 166.6, 172.2, 177.1, 179.9, 184, 188.9, 195.3, 201.6, 207.3, 215.303, 214.537, 218.056, 224.939, 229.594, 232.957, 236.736, 237.017, 240.007, 245.12, 251.107, 255.657, 258.811]) as AVG
  ),
    cpi20 as (
    select *
    from cpi
    where year = 2020
),t as (
    select
        id,
        ctype,
        event_day_date,
        geoid,
        nri_category,
        LEAST(extract(YEAR from event_day_date), 2020) as year, -- adjusting to 2020 dollars
        extract(MONTH from event_day_date) as month,
        damage,
        CASE WHEN extract(MONTH from event_day_date) = 1
                 THEN jan
             WHEN extract(MONTH from event_day_date) = 2
                 THEN feb
             WHEN extract(MONTH from event_day_date) = 3
                 THEN mar
             WHEN extract(MONTH from event_day_date) = 4
                 THEN apr
             WHEN extract(MONTH from event_day_date) = 5
                 THEN may
             WHEN extract(MONTH from event_day_date) = 6
                 THEN jun
             WHEN extract(MONTH from event_day_date) = 7
                 THEN jul
             WHEN extract(MONTH from event_day_date) = 8
                 THEN aug
             WHEN extract(MONTH from event_day_date) = 9
                 THEN sep
             WHEN extract(MONTH from event_day_date) = 10
                 THEN oct
             WHEN extract(MONTH from event_day_date) = 11
                 THEN nov
             WHEN extract(MONTH from event_day_date) = 12
                 THEN dec
            END AS month_2020_cpi
    from ${fusion_schema}.${table_name}_${view_id}, cpi20
    where ctype != 'population'
      and event_day_date is not null
),
     final as(
         select
             id,
             ctype,
             event_day_date,
             geoid,
             nri_category,
             damage,
             damage * month_2020_cpi /
             (
                 CASE
                     WHEN month = 1
                         THEN jan
                     WHEN month = 2
                         THEN feb
                     WHEN month = 3
                         THEN mar
                     WHEN month = 4
                         THEN apr
                     WHEN month = 5
                         THEN may
                     WHEN month = 6
                         THEN jun
                     WHEN month = 7
                         THEN jul
                     WHEN month = 8
                         THEN aug
                     WHEN month = 9
                         THEN sep
                     WHEN month = 10
                         THEN oct
                     WHEN month = 11
                         THEN nov
                     WHEN month = 12
                         THEN dec
                     END
                 ) adjusted_damage

         from t
                  LEFT JOIN cpi
                            ON t.year = cpi.year
     )

update ${fusion_schema}.${table_name}_${view_id} dst
set damage_adjusted = final.adjusted_damage
from final
where dst.id = final.id
`;

export const adjusted_dollar_pop = (table_name, view_id, fusion_schema) => `
update ${fusion_schema}.${table_name}_${view_id}
set damage_adjusted = damage
where ctype = 'population'
`;
