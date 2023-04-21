with county_to_state as (
  SELECT stusps, county.geoid, county.name, county.namelsad
  FROM geo.tl_2017_county_286 county
         JOIN geo.tl_2017_state_285 state
              on county.statefp::text = state.geoid
order by county.statefp, countyfp
  ), geoids as (
SELECT ihp.id, county_to_state.geoid
FROM open_fema_data.individuals_and_households_program_valid_registrations_v1_484 ihp
  JOIN county_to_state
ON damaged_state_abbreviation = stusps
  and (
  REPLACE(REPLACE(REPLACE(lower(county), '(', ''), ')', ''), ' ', '') like '%' || REPLACE(lower(namelsad), ' ', '') || '%'
  OR
  REPLACE(REPLACE(REPLACE(lower(county), '(', ''), ')', ''), ' ', '') = REPLACE(lower(name), ' ', '')
  )
  )
UPDATE open_fema_data.individuals_and_households_program_valid_registrations_v1_484 ihp
SET geoid = geoids.geoid
  FROM geoids
WHERE ihp.id = geoids.id
