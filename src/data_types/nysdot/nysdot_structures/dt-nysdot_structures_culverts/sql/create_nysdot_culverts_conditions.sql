/*
dama_dev_1=# select bin, count(1) from culverts group by bin having count(1) > 1;
   bin   | count 
---------+-------
 C87096H |     2
 C098848 |     2
 CA00336 |     2
 CA00151 |     2
(4 rows)

dama_dev_1=# select count(1) from culverts where stream_bed_m like '1%';
 count 
-------
   201
(1 row)

dama_dev_1=# select count(1) from culverts where stream_bed_m like '1%' and stream_bed_m <> '1 - No Waterway';
 count 
-------
     0
(1 row)

dama_dev_1=# select count(1) from nysdot_structures.nysdot_culverts_condition where nysdot_primary_owner_class is null;
 count 
-------
     0
(1 row)


*/

BEGIN ;

DROP VIEW IF EXISTS nysdot_structures.nysdot_culverts_condition CASCADE ;

CREATE VIEW nysdot_structures.nysdot_culverts_condition
  AS 
    SELECT DISTINCT ON (nysdot_bin)
        a.bin AS nysdot_bin,

        a.county AS nysdot_county,

        b.classification      AS  nysdot_primary_owner_class,
        a.primary_own         AS  nysdot_primary_owner,
        a.primary_mai         AS  nysdot_primary_maintenance,

        a.condition_r         AS  nysdot_condition_rating,
        (a.condition_r < 5)   AS  nysdot_is_in_poor_condition,
        a.crossed             AS  nysdot_crossed,

        a.stream_bed_m        AS  nysdot_stream_bed_material,
        
        ( a.stream_bed_m <> '1 - No Waterway' ) AS nysdot_crosses_water,

        public.ST_X(public.ST_StartPoint(a.wkb_geometry)) AS nysdot_longitude,
        public.ST_Y(public.ST_StartPoint(a.wkb_geometry)) AS nysdot_latitude

      FROM nysdot_structures.culverts AS a
        
        LEFT OUTER JOIN nysdot_structures.government_agency_ownership_classifications AS b
          ON ( a.primary_own = b.name )

      ORDER BY
          nysdot_bin,
          a.last_inspec DESC,
          location_la DESC NULLS LAST
;

COMMIT ;
