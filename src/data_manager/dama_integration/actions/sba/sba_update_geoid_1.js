export const sql = (table_name) => `
  with geo as (
    select c.geoid, c.statefp, s.stusps, c.namelsad county_name, s.name state_name
    from geo.tl_2017_us_county_4326 c
             join public.tl_2017_us_state s
                  on s.statefp = c.statefp
),
     newGeo as (
         SELECT entry_id, fema_disaster_number,
                damaged_property_county_or_parish_name, damaged_property_state_code, total_verified_loss,
                sba.geoid sba_geoid, geo.geoid geo_geoid, geo.county_name
         FROM public.sba_disaster_loan_data_new sba
                  join geo
                       on(
                                 lower(REPLACE(geo.county_name, '' '', ''''))
                                 like
                                 ''%'' || replace(
                                         replace(
                                                 replace(
                                                         replace(
                                                                 replace(
                                                                         lower(sba.damaged_property_county_or_parish_name),
                                                                         ''st '', ''st. ''),
                                                                 ''saint '', ''st.''),
                                                         ''ó'', ''o''),
                                                 ''territory of '', ''''),
                                         '' '', '''') || ''%''
                             )
                           and geo.stusps = sba.damaged_property_state_code
         where sba.geoid is null
         order by entry_id, geo.state_name, geo.county_name
     )



update public.sba_disaster_loan_data_new dst
set geoid = newGeo.geo_geoid
from newGeo
where dst.entry_id = newGeo.entry_id
  and dst.geoid is null

-- there are still null geoids. for them:
-- use state centroid county/ first county (001)
-- use disaster number to get geoid?
  `
