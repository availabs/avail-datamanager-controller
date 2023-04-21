export const ihpPostProcessing = ({county_schema, county_table, state_schema, state_table, ihp_schema, ihp_table}) => `
 with county_to_state as (
    SELECT stusps, county.geoid, county.name, county.namelsad
    FROM ${county_schema}.${county_table} county
             JOIN ${state_schema}.${state_table} state
                  on lpad(county.statefp::text, 2, '0') = state.geoid
    order by county.statefp, countyfp
),

t as (SELECT
	  ihp.id,
--     stusps,
    county_to_state.geoid
--     name,
--     namelsad
FROM ${ihp_schema}.${ihp_table} ihp
 join county_to_state
                         on damaged_state_abbreviation = stusps
                             and (
                                        REPLACE(REPLACE(REPLACE(lower(county), '(', ''), ')', ''), ' ', '') like '%' || REPLACE(lower(namelsad), ' ', '') || '%'
                                    OR
                                        REPLACE(REPLACE(REPLACE(lower(county), '(', ''), ')', ''), ' ', '') = REPLACE(lower(name), ' ', '')
                                ))

update ${ihp_schema}.${ihp_table} dst
set geoid = t.geoid
from t
where t.id = dst.id










`
