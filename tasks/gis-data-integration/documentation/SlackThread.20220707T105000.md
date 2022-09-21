# Slack Thread

Monologue Paul to Alex.

```log
The TableDescriptor gets sent to the client.
The client can edit it then send it back.
Currently, I only allow editing tableName and columnTypes.col (the column name).

10:51
That's the table/form

10:52
We'll need to eventually add editing db_type too.

10:53
The current column type algorithm would make all FIPS codes INTEGERS.

10:53
Also, if all seen values for a shapefile field are NULL, the column type
defaults to TEXT.

10:56
promoteToMulti controls the ogr2ogr PROMOTE_TO_MULTI flag.
If there is a combination of LineStrings and MultiLineStrings the loader will
by default use the MultiLineString geometry type in the DB.
The alternative is to leave the heterogeneous types and use Geometry as the
column type.

10:58
The forcePostGisDimension flag is similar. It controls the coordinate dimension
(XY, XYZ, XYM, and XYZM)

10:59
The default behavior is the same as what I do when loading datasets manually.

10:59
Except I respond to error messages where the loader is more proactive.

11:00
The default tippecanoe args

11:01
Supporting layers would be a bit complicated.

11:01
But not impossible if needed.

11:02
ETL dir structure:
tree etl-work-dir/ff6eeb3d-eedc-4d71-be9f-b17b4b05eaa9
etl-work-dir/ff6eeb3d-eedc-4d71-be9f-b17b4b05eaa9
├── dataset
│ └── marinehighways
│ ├── MarineHighways.dbf
│ ├── MarineHighways.prj
│ ├── MarineHighways.shp
│ └── MarineHighways.shx
├── dataset_upload_metadata.json
├── geodataset_metadata.json
├── layer_0
│ ├── layer_analysis.json
│ ├── layer_geometries.geojson.gz
│ ├── layer.mbtiles
│ ├── logs
│ │ ├── create_table.20220706T234120372Z.sql
│ │ ├── load_table_metadata.20220706T234120372Z.json
│ │ └── tippecanoe.20220706T234120932Z.json
│ ├── STATUS
│ └── table_descriptor.sql
├── layerNameToId.json
└── marinehighways.tgz

4 directories, 16 files

11:02
That dir is in the avail-falcor root.

11:03
Lots of logging to help debugging.

11:06
This pattern is pretty useful: DB backed iterator
```
