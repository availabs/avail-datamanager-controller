# OSM Roadways

Identifying OSM Roadways

```sql
SELECT (
   (
     REPLACE(tags->>'highway', '_link', '') IN (
       'motorway',
       'trunk',
       'primary',
       'secondary',
       'tertiary',

       'unclassified',
       'residential',
       'living_street'
     )
   )
   OR
   (
     ( tags->>'highway' = 'service' )
     AND
     (
       tags->>'service' NOT IN (
         'parking',
         'driveway',
         'drive-through'
       )
     )
   )
) ;
```

See https://github.com/availabs/NPMRDS_Database/blob/176b1d58e7463c99e3143b7cebc7e64aa1348f4c/sql/osm/create_osm_way_is_roadway_fn.sql
