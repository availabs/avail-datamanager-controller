# NYSDOT RIS Issues

## Multiple database schemas

```
npmrds_production=# \d
                                    List of relations
 Schema |                     Name                     |       Type        |    Owner     
--------+----------------------------------------------+-------------------+--------------
 ris    | conflation_ris                               | table             | npmrds_admin
 ris    | cr                                           | view              | npmrds_admin
 ris    | geo_lookup                                   | table             | npmrds_admin
 ris    | geo_lookup_v0_4_2                            | table             | npmrds_admin
 ris    | geo_lookup_v0_5_5                            | table             | npmrds_admin
 ris    | nys_ris_2019_v20190524                       | table             | npmrds_admin
 ris    | ris_2016_agg                                 | materialized view | npmrds_admin
 ris    | ris_2017_agg                                 | materialized view | npmrds_admin
 ris    | ris_2017_with_dir                            | view              | npmrds_admin
 ris    | ris_2018_agg                                 | materialized view | npmrds_admin
 ris    | ris_2019_agg                                 | materialized view | npmrds_admin
 ris    | ris_2019_agg_bak                             | materialized view | npmrds_admin
 ris    | road_inventory_system_2016                   | table             | npmrds_admin
 ris    | road_inventory_system_2017                   | table             | npmrds_admin
 ris    | road_inventory_system_2018                   | table             | npmrds_admin
 ris    | road_inventory_system_2018_bak               | table             | npmrds_admin
 ris    | road_inventory_system_2019                   | view              | npmrds_admin
 ris    | road_inventory_system_2019_bak2              | view              | npmrds_admin
 ris    | road_inventory_system_2019_bak_20210202t1625 | table             | npmrds_admin
 ris    | road_inventory_system_2019_old               | view              | npmrds_admin
 ris    | road_inventory_system_2019_pre_patch         | table             | npmrds_admin
 ris    | tmp_ris_2019_aadts_patch                     | table             | npmrds_admin
 ris    | tmp_road_inventory_system_2019_patched_aadts | table             | npmrds_admin
(23 rows)
```

NOTE: It would be nice to remove the "\_gdb" suffix from the below table names.

```
npmrds_production=# \d
                                       List of relations
   Schema   |                          Name                           |   Type   |    Owner     
------------+---------------------------------------------------------+----------+--------------
 nysdot_ris | nys_roadway_inventory_system_v20160000_gdb              | table    | npmrds_admin
 nysdot_ris | nys_roadway_inventory_system_v20160000_gdb_ogc_fid_seq  | sequence | npmrds_admin
 nysdot_ris | nys_roadway_inventory_system_v20170000_gdb              | table    | npmrds_admin
 nysdot_ris | nys_roadway_inventory_system_v20170000_gdb_ogc_fid_seq  | sequence | npmrds_admin
 nysdot_ris | nys_roadway_inventory_system_v20180000_gdb              | table    | npmrds_admin
 nysdot_ris | nys_roadway_inventory_system_v20180000_gdb_ogc_fid_seq  | sequence | npmrds_admin
 nysdot_ris | nys_roadway_inventory_system_v20190000_gdb              | table    | npmrds_admin
 nysdot_ris | nys_roadway_inventory_system_v20190000_gdb_ogc_fid_seq  | sequence | npmrds_admin
 nysdot_ris | nys_roadway_inventory_system_v20200921_gdb              | table    | npmrds_admin
 nysdot_ris | nys_roadway_inventory_system_v20200921_gdb_objectid_seq | sequence | npmrds_admin
 nysdot_ris | nys_roadway_inventory_system_v20210800_gdb              | table    | npmrds_admin
 nysdot_ris | nys_roadway_inventory_system_v20210800_gdb_objectid_seq | sequence | npmrds_admin
(12 rows)
```

```
                                           List of relations
   Schema   |                         Name                          |       Type
------------+-------------------------------------------------------+-------------------
 ...
 conflation | nys_ris_2019                                          | table
 conflation | nys_ris_2019_bak                                      | table
 conflation | nys_ris_2019_before_srid_fix                          | table
 conflation | nys_ris_2019_corrected                                | table
 conflation | nys_ris_2019_new_objectid_seq                         | sequence
 conflation | nys_ris_2019_objectid_seq                             | sequence
 conflation | nys_ris_2019_old                                      | table
 conflation | nys_traffic_counts_station_year_directions            | view
 ...
 ```
