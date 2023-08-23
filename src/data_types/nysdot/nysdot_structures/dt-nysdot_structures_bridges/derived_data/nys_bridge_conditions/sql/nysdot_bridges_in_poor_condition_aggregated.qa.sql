/*
dama_dev_1=# select count(1) from nysdot_usdot_bridges_condition where COALESCE(nysdot_primary_owner_class, usdot_owner_class) = 'Other' and nysdot_primary_owner_class is null;
 count 
-------
     0
(1 row)

dama_dev_1=# select count(1) from nysdot_usdot_bridges_condition where COALESCE(nysdot_primary_owner_class, usdot_owner_class) = 'Other' and usdot_owner_class is null;
 count 
-------
   243
(1 row)

dama_dev_1=# select count(1) from nysdot_usdot_bridges_condition where COALESCE(nysdot_primary_owner_class, usdot_owner_class) = 'Other' and usdot_owner_class is not null;
 count 
-------
     1
(1 row)
*/

-- SELECT
--     SUM(total_bridges)
--   FROM (
--     SELECT
--         county,
--         (
--           + total_bridges_federal_owned
--           + total_bridges_state_owned
--           + total_bridges_county_owned
--           + total_bridges_municipality_owned
--           + total_bridges_auth_or_comm_owned
--           + total_bridges_railroad_owned
--           + total_bridges_other_owned
--         ) AS total_bridges
--       FROM nysdot_structures.nysdot_bridges_in_poor_condition_aggregated
--   ) AS a;
--   sum  
-- -------
--  20038
-- (1 row)

-- SELECT
--     COUNT(1)
--   FROM nysdot_structures.nysdot_usdot_bridges_condition
-- ;
--  count 
-- -------
--  20038
-- (1 row)

-- dama_dev_1=# select count(1) from (select bin from nysdot_structures.bridges union select RegExp_Replace(structure, '^0{1,}', '') from us_bureau_of_transportation_statistics.ny_bridge_inventory ) as a
-- dama_dev_1-# ;
--  count 
-- -------
--  20038
-- (1 row)

