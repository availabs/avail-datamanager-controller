# Inspecting The M Dimension

https://stackoverflow.com/a/67531720/3970755

https://postgis.net/docs/manual-3.1/ST_M.html

```sql
WITH j AS (
  SELECT
      (ST_DumpPoints(wkb_geometry)).geom AS p
    FROM ui_loaded_data_sources.pavements_2014
)
  SELECT ST_M(p)
    FROM j
    WHERE ( ST_M(p) IS NOT NULL );
```
