/*
dama_dev_1=# select nysdot_county, usdot_county, count(1) from nysdot_usdot_bridges_condition where nysdot_county <> usdot_county group by 1,2;
 nysdot_county | usdot_county | count 
---------------+--------------+-------
 WASHINGTON    | COLUMBIA     |     1
(1 row)

dama_dev_1=# select count(1) from nysdot_usdot_bridges_condition where nysdot_county = usdot_county;
 count 
-------
 17509
(1 row)

dama_dev_1=# select count(1) from nysdot_usdot_bridges_condition where (nysdot_county || usdot_county) IS NULL;
 count 
-------
  2528
(1 row)

dama_dev_1=# select count(1) from nysdot_usdot_bridges_condition where nysdot_county <> usdot_county;
 count 
-------
     1
(1 row)

dama_dev_1=# select count(1) from nysdot_usdot_bridges_condition ;
 count 
-------
 20038
(1 row)

dama_dev_1=# select 17509 + 2528 + 1;
 ?column? 
----------
    20038
(1 row)

*/

BEGIN ;

DROP VIEW IF EXISTS nysdot_structures.nysdot_usdot_bridges_condition ;

CREATE VIEW nysdot_structures.nysdot_usdot_bridges_condition
  AS 
    SELECT
        a.nysdot_bin,
        a.usdot_structure_008,

        a.nysdot_county,

        a.nysdot_aadt,

        b.classification      AS  nysdot_primary_owner_class,
        a.nysdot_primary_own  AS  nysdot_primary_owner,
        a.nysdot_primary_mai  AS  nysdot_primary_maintenance,

        a.nysdot_condition_r        AS  nysdot_condition_rating,
        (a.nysdot_condition_r < 5)  AS nysdot_is_in_poor_condition,
        a.nysdot_crossed,

        public.ST_X(public.ST_StartPoint(wkb_geometry)) AS nysdot_longitude,
        public.ST_Y(public.ST_StartPoint(wkb_geometry)) AS nysdot_latitude,

        h.county_name         AS  usdot_county,
        a.usdot_location_0    AS  usdot_location_009,
        a.usdot_adt_029,
        a.usdot_year_adt_0    AS usdot_year_adt_030,
        a.usdot_detour_kil    AS usdot_detour_km_019,
        
        a.usdot_maintenanc    AS  usdot_maintenance_owner_021,
        d.classification      AS  usdot_maintainer_class,
        e.description         AS  usdot_maintainer_description,

        a.usdot_owner_022,
        f.classification      AS  usdot_owner_class,
        g.description         AS  usdot_owner_description,
        
        a.usdot_deck_cond     AS  usdot_deck_condition_058,
        a.usdot_superstruc    AS  usdot_superstructure_condition_059,
        a.usdot_substructu    AS  usdot_substructure_condition_060,
        a.usdot_channel_co    AS  usdot_channel_condition_061,
        a.usdot_culvert_co    AS  usdot_culvert_condition_062,

        (
          LEAST(
            NULLIF(a.usdot_deck_cond,   'N')::INTEGER,
            NULLIF(a.usdot_superstruc,  'N')::INTEGER,
            NULLIF(a.usdot_substructu,  'N')::INTEGER,
            NULLIF(a.usdot_channel_co,  'N')::INTEGER,
            NULLIF(a.usdot_culvert_co,  'N')::INTEGER
          ) < 5 
        ) AS usdot_is_in_poor_condition,

        a.usdot_service_un    AS  usdot_type_of_service_under_bridge_code_042b,
        c.description         AS  usdot_type_of_service_under_bridge_description,
        (usdot_service_un::SMALLINT BETWEEN 5 AND 9) AS usdot_crosses_water,
        a.usdot_features_d    AS  usdot_features_desc_006a,

        public.ST_X(public.ST_EndPoint(wkb_geometry)) AS usdot_longitude,
        public.ST_Y(public.ST_EndPoint(wkb_geometry)) AS usdot_latitude,

        a.distance_meters AS nysdot_usdot_location_difference_meters

      FROM nysdot_structures.nysdot_usdot_bridges_join AS a
        
        LEFT OUTER JOIN nysdot_structures.government_agency_ownership_classifications AS b
          ON ( a.nysdot_primary_own = b.name )

        LEFT OUTER JOIN us_bureau_of_transportation_statistics.type_of_service_under_bridge_codes AS c
          ON ( a.usdot_service_un = c.code )

        LEFT OUTER JOIN us_bureau_of_transportation_statistics.government_agency_ownership_classifications AS d
          ON ( a.usdot_maintenanc = d.code )

        LEFT OUTER JOIN us_bureau_of_transportation_statistics.government_agency_ownership_codes AS e
          ON ( a.usdot_maintenanc = e.code )

        LEFT OUTER JOIN us_bureau_of_transportation_statistics.government_agency_ownership_classifications AS f
          ON ( a.usdot_owner_022 = f.code )

        LEFT OUTER JOIN us_bureau_of_transportation_statistics.government_agency_ownership_codes AS g
          ON ( a.usdot_owner_022 = g.code )

        LEFT OUTER JOIN (
          SELECT DISTINCT
              x.geoid,
              REGEXP_REPLACE(
                TRIM(REPLACE(UPPER(x.namelsad), 'COUNTY', '')),
                '[^A-Z ]',
                '',
                'g'
              ) AS county_name
            FROM us_census_tiger.county AS x
            WHERE ( x.geoid LIKE '36%' )
        ) AS h
          ON ( ( a.usdot_state_code || a.usdot_county_cod ) = h.geoid )
;

COMMIT ;
