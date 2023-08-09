# NYSDOT Roadway Inventory System Derived Data

## Total roadway miles by jurisdiction

```sql
SELECT
    jurisdiction,
    ROUND(SUM(section_length)::NUMERIC, 1) AS total_miles,
    ROUND(SUM(section_length * COALESCE(total_lanes, 2))::NUMERIC, 1) AS total_lane_miles
  FROM nysdot_roadway_inventory_system.roadway_inventory_system
  GROUP BY 1
  ORDER BY 3 DESC
;
          jurisdiction           | total_miles | total_lane_miles 
---------------------------------+-------------+------------------
 03 Town                         |     58087.2 |         114833.5
 02 County                       |     20202.7 |          41668.0
 01 NYSDOT                       |     17984.5 |          40414.1
 04 City or village              |     18166.5 |          36015.1
 31 NYS Thruway                  |      1415.7 |           2817.0
 90 Public - Unclaimed           |       535.5 |            979.7
 21 Other State agencies         |       495.0 |            884.9
 74 Army                         |       481.4 |            721.4
 11 State Parks                  |       347.3 |            697.5
 26 Private or Restricted Access |       377.0 |            667.8
 25 Other local agencies         |       133.8 |            283.0
 12 Local Parks                  |       139.1 |            256.4
 32 Other Toll Authority         |        78.4 |            202.4
 62 Bureau of Indian Affairs     |        97.9 |            184.0
 40 Other Public Instrumentality |        88.6 |            169.7
 50 Indian Tribal Government     |        79.1 |            154.8
 41 Local Service                |        71.9 |            136.5
 91 Public Restricted            |        47.5 |             89.6
 92 Slip Lane                    |        69.3 |             72.8
 66 National Park Service        |        42.1 |             71.6
 60 Other Federal agencies       |        36.9 |             71.5
 73 Navy/Marines                 |        11.0 |             21.2
 80 Other                        |         8.0 |             17.9
 95 Non-Mainline - Connector     |        10.5 |             16.8
 70 Corps of Engineers (Civil)   |         6.6 |              9.0
 63 Bureau of Fish and Wildlife  |         6.4 |              7.7
 93 Roundabout                   |         5.4 |              6.9
 64 U.S. Forest Service          |         0.6 |              0.9
 27 Railroad                     |         0.4 |              0.7
 72 Air Force                    |         0.2 |              0.2
 94 Non-Mainline - Other         |         0.2 |              0.2
(31 rows)
```

* [sql](./miles_by_jurisdiction.sql)
* [csv](./miles_by_jurisdiction.csv)

## Total roadway miles by owning_jurisdiction

```sql
SELECT
    owning_jurisdiction,
    ROUND(SUM(section_length)::NUMERIC, 1) AS total_miles,
    ROUND(SUM(section_length * COALESCE(total_lanes, 2))::NUMERIC, 1) AS total_lane_miles
  FROM nysdot_roadway_inventory_system.roadway_inventory_system
  GROUP BY 1
  ORDER BY 3 DESC
;
       owning_jurisdiction       | total_miles | total_lane_miles 
---------------------------------+-------------+------------------
 03 Town                         |     58124.6 |         114898.6
 02 County                       |     20245.5 |          41728.7
 01 NYSDOT                       |     17925.7 |          40195.3
 04 City or village              |     18192.0 |          35990.8
 31 NYS Thruway                  |      1404.5 |           2784.0
 11 State Parks                  |       468.5 |           1076.7
 90 Public - Unclaimed           |       535.3 |            979.0
 21 Other State agencies         |       513.5 |            920.1
 74 Army                         |       481.4 |            721.4
 26 Private or Restricted Access |       377.7 |            669.2
 25 Other local agencies         |       134.7 |            284.7
 12 Local Parks                  |       148.1 |            275.7
 32 Other Toll Authority         |        78.6 |            202.8
 62 Bureau of Indian Affairs     |       105.2 |            198.8
 40 Other Public Instrumentality |        89.3 |            170.9
 50 Indian Tribal Government     |        79.1 |            154.8
 66 National Park Service        |        42.6 |             72.6
 60 Other Federal agencies       |        37.4 |             72.5
 73 Navy/Marines                 |        11.0 |             21.2
 80 Other                        |         8.0 |             17.9
 41 Local Service                |         6.4 |             12.4
 70 Corps of Engineers (Civil)   |         6.6 |              9.0
 63 Bureau of Fish and Wildlife  |         6.4 |              7.7
 91 Public Restricted            |         3.1 |              6.1
 64 U.S. Forest Service          |         0.6 |              0.9
 27 Railroad                     |         0.4 |              0.7
 72 Air Force                    |         0.2 |              0.2
 93 Roundabout                   |         0.0 |              0.1
 95 Non-Mainline - Connector     |         0.1 |              0.1
(29 rows)
```

* [sql](./miles_by_owning_jurisdiction.sql)
* [csv](./miles_by_owning_jurisdiction.csv)

