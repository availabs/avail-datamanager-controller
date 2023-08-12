# NYSDOT Roadway Inventory System

## QA

### Verify section_lengths sum in PostgreSQL is same as in original FileGDB.

```sql
dama_dev_1=# select sum(section_length) from roadway_inventory_system ;
       sum        
------------------
 119026.359999994
(1 row)
```

```sh
$ ogrinfo -sql 'SELECT SUM(section_length) FROM roadway_inventory_system' nys-roadway-inventory-system-v20210800.gdb

Had to open data source read-only.
INFO: Open of `nys-roadway-inventory-system-v20210800.gdb'
      using driver `OpenFileGDB' successful.

Layer name: roadway_inventory_system
Geometry: None
Feature Count: 1
Layer SRS WKT:
(unknown)
SUM_section_length: Real (0.0)
OGRFeature(roadway_inventory_system):0
  SUM_section_length (Real) = 119026.359999971
```
