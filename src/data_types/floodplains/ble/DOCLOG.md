# Floodplains Base Level Engineering

## Resources

https://www.fema.gov/about/reports-and-data
* https://www.fema.gov/about/glossary

https://www.fema.gov/sites/default/files/documents/fema_when-use-ble.pdf

https://www.fema.gov/sites/default/files/documents/fema_base-level-engineering-analysis-mapping_112022.pdf

https://www.fema.gov/sites/default/files/2020-02/Domain_Tables_Technical_Reference_Feb_2019.pdf

https://webapps.usgs.gov/infrm/pubs/202107_v6_BLE%20Submittal%20Guidance_R6___FINAL.pdf

## Incorrect Projection for WNY_Spatial_Files_3/04120104/BLE_04120104.gdb

The projection for the WNY_Spatial_Files_3/04120104/BLE_04120104.gdb was incorrect.

As-is, with projection the PROJCS is "NAD83 / California zone 1 (ftUS)",
the geodatabase features are in the Pacific Ocean, off the California coast.

```sh
$ ogrinfo -so BLE_04120104.gdb FP_0_2PCT
INFO: Open of `BLE_04120104.gdb'
      using driver `OpenFileGDB' successful.

Layer name: FP_0_2PCT
Geometry: Multi Polygon
Feature Count: 208
Extent: (1195533.747756, 963333.202582) - (1268367.446577, 1046581.780507)
Layer SRS WKT:
PROJCS["NAD83 / California zone 1 (ftUS)",
    GEOGCS["NAD83",
        DATUM["North_American_Datum_1983",
            SPHEROID["GRS 1980",6378137,298.257222101,
                AUTHORITY["EPSG","7019"]],
            TOWGS84[0,0,0,0,0,0,0],
            AUTHORITY["EPSG","6269"]],
        PRIMEM["Greenwich",0,
            AUTHORITY["EPSG","8901"]],
        UNIT["degree",0.0174532925199433,
            AUTHORITY["EPSG","9122"]],
        AUTHORITY["EPSG","4269"]],
    PROJECTION["Lambert_Conformal_Conic_2SP"],
    PARAMETER["standard_parallel_1",41.66666666666666],
    PARAMETER["standard_parallel_2",40],
    PARAMETER["latitude_of_origin",39.33333333333334],
    PARAMETER["central_meridian",-122],
    PARAMETER["false_easting",6561666.667],
    PARAMETER["false_northing",1640416.667],
    UNIT["US survey foot",0.3048006096012192,
        AUTHORITY["EPSG","9003"]],
    AXIS["X",EAST],
    AXIS["Y",NORTH],
    AUTHORITY["EPSG","2225"]]
FID Column = OBJECTID
Geometry Column = SHAPE
BLE_ID: String (25.0)
VERSION_ID: String (25.0)
FP_AR_ID: String (25.0)
SOURCE_CIT: String (11.0)
SHAPE_Length: Real (0.0)
SHAPE_Area: Real (0.0)
```

NOTE:

* "NAD83 / California zone 1 (ftUS)" is [EPSG:2225](https://epsg.io/2225)
* "NAD83(2011) / New York West (ftUS)" is [EPSG:6541](https://epsg.io/6541)

We use the ogr2ogr [a_srs](https://gdal.org/programs/ogr2ogr.html#cmdoption-ogr2ogr-a_srs) flag
to assign the SRS to EPSG:6541. We don't want to reproject because the original is incorrect.

> -a_srs <srs_def>
>
> Assign an output SRS, but without reprojecting (use -t_srs to reproject)

This assignment happens in the ./src/createBLEMergedGDBF.js script.
The ogr2ogr command would be as follows:

```sh
$ ogr2ogr -a_srs EPSG:6541 -F 'ESRI Shapefile' BLE_04120104 BLE_04120104.gdb
```

## Inconsistent shape_leng units

```sql
CREATE TEMPORARY TABLE tmp_ble_fp_pct_perimters
  AS
    SELECT
        fid,
        public.ST_Perimeter(
          public.GEOGRAPHY(
            public.ST_Transform(
              shape,
              4326
            )
          )
        ) AS perimeter_meters
      FROM floodplains.merged_ble_fp_pcts
;

SELECT
    gfid AS data_source,
    ROUND((a.shape_leng / b.perimeter_meters)::NUMERIC, 2) AS "shape_leng units/meter",
    COUNT(1)
  FROM merged_ble_fp_pcts AS a
    INNER JOIN tmp_ble_fp_pct_perimters AS b
  USING (fid)
  GROUP BY 1,2
  ORDER BY 1,2
;

/* RESULT:
                                 data_source                                  | shape_leng units/meter | count
------------------------------------------------------------------------------+------------------------+-------
 WNY_Spatial_Files_1/02050104/BLE_02050104.gdb::FP_01PCT                      |                   3.28 |    21
 WNY_Spatial_Files_1/02050104/BLE_02050104.gdb::FP_0_2PCT                     |                   3.28 |    19
 WNY_Spatial_Files_1/02050105/BLE_02050105.gdb::FP_01PCT                      |                   3.28 |     7
 WNY_Spatial_Files_1/02050105/BLE_02050105.gdb::FP_0_2PCT                     |                   3.28 |     7
 WNY_Spatial_Files_1/05010001/BLE_05010001.gdb::FP_01PCT                      |                   3.28 |   355
 WNY_Spatial_Files_1/05010001/BLE_05010001.gdb::FP_0_2PCT                     |                   3.28 |   273
 WNY_Spatial_Files_1/05010002/BLE_05010002-Conewango.gdb::FP_01PCT            |                   0.00 |   886
 WNY_Spatial_Files_1/05010002/BLE_05010002-Conewango.gdb::FP_0_2PCT           |                   0.00 |   814
 WNY_Spatial_Files_1/05010004/BLE_05010004-French.gdb::FP_01PCT               |                   0.00 |   238
 WNY_Spatial_Files_1/05010004/BLE_05010004-French.gdb::FP_0_2PCT              |                   0.00 |   213
 WNY_Spatial_Files_2/04130002/BLE_04130002.gdb::FP_01PCT                      |                   3.26 |     1
 WNY_Spatial_Files_2/04130002/BLE_04130002.gdb::FP_01PCT                      |                   3.28 |   475
 WNY_Spatial_Files_2/04130002/BLE_04130002.gdb::FP_0_2PCT                     |                   3.27 |     1
 WNY_Spatial_Files_2/04130002/BLE_04130002.gdb::FP_0_2PCT                     |                   3.28 |   408
 WNY_Spatial_Files_2/04130003/BLE_04130003.gdb::FP_01PCT                      |                   3.28 |   143
 WNY_Spatial_Files_2/04130003/BLE_04130003.gdb::FP_0_2PCT                     |                   3.28 |   133
 WNY_Spatial_Files_3/04120101/BLE_04120101-Chautauqua-Conneaut.gdb::FP_01PCT  |                   0.00 |   490
 WNY_Spatial_Files_3/04120101/BLE_04120101-Chautauqua-Conneaut.gdb::FP_0_2PCT |                   0.00 |   466
 WNY_Spatial_Files_3/04120102/BLE_04120102.gdb::FP_01PCT                      |                   3.28 |   170
 WNY_Spatial_Files_3/04120102/BLE_04120102.gdb::FP_0_2PCT                     |                   3.28 |   139
 WNY_Spatial_Files_3/04120103/BLE_04120103.gdb::FP_01PCT                      |                   3.28 |    72
 WNY_Spatial_Files_3/04120103/BLE_04120103.gdb::FP_0_2PCT                     |                   3.28 |    59
 WNY_Spatial_Files_3/04120104/BLE_04120104.gdb::FP_01PCT                      |                   3.28 |   231
 WNY_Spatial_Files_3/04120104/BLE_04120104.gdb::FP_0_2PCT                     |                   3.28 |   208
(24 rows)
*/

CREATE temporary table tmp_ble_fp_pct_perimters
  AS
    SELECT
        fid,
        public.ST_Perimeter(
          public.ST_Transform(shape, 4326)
        ) AS perimeter_4326
      FROM floodplains.merged_ble_fp_pcts
;

SELECT
    gfid AS data_source,
    MIN(ROUND((shape_leng / perimeter_4326)::NUMERIC, 7)) AS ratio_min,
    MAX(ROUND((shape_leng / perimeter_4326)::NUMERIC, 7)) AS ratio_max
  FROM merged_ble_fp_pcts AS a
    INNER JOIN tmp_ble_fp_pct_perimters AS b
      USING (fid)
  WHERE (
    gfid IN (
      'WNY_Spatial_Files_1/05010002/BLE_05010002-Conewango.gdb::FP_01PCT',
      'WNY_Spatial_Files_1/05010002/BLE_05010002-Conewango.gdb::FP_0_2PCT',
      'WNY_Spatial_Files_1/05010004/BLE_05010004-French.gdb::FP_01PCT',
      'WNY_Spatial_Files_1/05010004/BLE_05010004-French.gdb::FP_0_2PCT',
      'WNY_Spatial_Files_3/04120101/BLE_04120101-Chautauqua-Conneaut.gdb::FP_01PCT',
      'WNY_Spatial_Files_3/04120101/BLE_04120101-Chautauqua-Conneaut.gdb::FP_0_2PCT'
    )
  )
  GROUP BY 1
  ORDER BY 1
;

/* RESULT
                                 data_source                                  | ratio_min | ratio_max
------------------------------------------------------------------------------+-----------+-----------
 WNY_Spatial_Files_1/05010002/BLE_05010002-Conewango.gdb::FP_01PCT            | 0.9999997 | 1.0000004
 WNY_Spatial_Files_1/05010002/BLE_05010002-Conewango.gdb::FP_0_2PCT           | 0.9999997 | 1.0000002
 WNY_Spatial_Files_1/05010004/BLE_05010004-French.gdb::FP_01PCT               | 0.9999997 | 1.0000002
 WNY_Spatial_Files_1/05010004/BLE_05010004-French.gdb::FP_0_2PCT              | 0.9999997 | 1.0000004
 WNY_Spatial_Files_3/04120101/BLE_04120101-Chautauqua-Conneaut.gdb::FP_01PCT  | 0.9999998 | 1.0000003
 WNY_Spatial_Files_3/04120101/BLE_04120101-Chautauqua-Conneaut.gdb::FP_0_2PCT | 0.9999998 | 1.0000003
(6 rows)
*/
```

## The extra `BLE_ID` column

The BLE_ID column does not appear to have an equivalent in the
[FIRM Map Database](https://www.fema.gov/sites/default/files/2020-02/FIRM_Database_Technical_Reference_Feb_2019.pdf) Schema.

The current way we're handling that column is to include the BLE_ID value in the GFID column,
where the GFID value has the following encoding:

```
<Source GIS Dataset>::<Dataset Layer Name>::<BLE_ID>`
```

## S_Fld_Haz_Ar Field Types

From [FIRM Database Technical Reference](https://www.fema.gov/sites/default/files/2020-02/FIRM_Database_Technical_Reference_Feb_2019.pdf), Table 13: S_Fld_Haz_Ar:

| Field | R/A | Type | Length/Precision | Scale (SHP Only) | Joined Spatial / Lookup Tables or Domains
|---|---|---|---|---|---|
DFIRM_ID     | R   | Text     | 6         |                   | N/A
VERSION_ID   | R   | Text     | 11        |                   | N/A
FLD_AR_ID    | R   | Text     | 25        |                   | N/A
STUDY_TYP    | R   | Text     | 38        |                   | D_Study_Typ
FLD_ZONE     | R   | Text     | 17        |                   | D_Zone
ZONE_SUBTY   | A   | Text     | 72        |                   | D_Zone_Subtype
SFHA_TF      | R   | Text     | 1         |                   | D_TrueFalse
STATIC_BFE   | A   | Double   | Default   | 2                 | N/A
V_DATUM      | A   | Text     | 17        |                   | D_V_Datum
DEPTH        | A   | Double   | Default   | 2                 | N/A
LEN_UNIT     | A   | Text     | 16        |                   | D_Length_Units
VELOCITY     | A   | Double   | Default   | 2                 | N/A
VEL_UNIT     | A   | Text     | 20        |                   | D_Velocity_Units
AR_REVERT    | A   | Text     | 17        |                   | D_Zone
AR_SUBTRV    | A   | Text     | 72        |                   | D_Zone_Subtype
BFE_REVERT   | A   | Double   | Default   | 2                 | N/A
DEP_REVERT   | A   | Double   | Default   | 2                 | N/A
DUAL_ZONE    | A   | Text     | 1         |                   | D_TrueFalse
SOURCE_CIT   | R   | Text     | 11        |                   | L_Source_Cit

```js
const gdb_fields = {
  dfirm_id: "text",
  version_id: "text",
  fld_ar_id: "text",
  study_typ: "text",
  fld_zone: "text",
  zone_subty: "text",
  sfha_tf: "text",
  static_bfe: "double",
  v_datum: "text",
  depth: "double",
  len_unit: "text",
  velocity: "double",
  vel_unit: "text",
  ar_revert: "text",
  ar_subtrv: "text",
  bfe_revert: "double",
  dep_revert: "double",
  dual_zone: "text",
  source_cit: "text",
  gfid: "text",
  shape_leng: "double",
  shape_area: "double",
};
```

## BLE fld_haz_ar NULL Geometries

In the fld_haz_ar layers, there are two features with NULL or empty geometries.

```SQL
DROP TABLE IF EXISTS qa_ble_fld_haz_ar_perimeters ;

CREATE TABLE qa_ble_fld_haz_ar_perimeters
  AS
    SELECT
        fid,
        public.ST_Perimeter(
          public.GEOGRAPHY(
            public.ST_Transform(
              shape,
              4326
            )
          )
        ) AS perimeter_meters
      FROM floodplains.merged_ble_fld_haz_ar
;

SELECT
    gfid AS data_source,
    ROUND((a.shape_leng / b.perimeter_meters)::NUMERIC, 2) AS "shape_leng units/meter",
    COUNT(1)
  FROM merged_ble_fld_haz_ar AS a
    INNER JOIN qa_ble_fld_haz_ar_perimeters AS b
  USING (fid)
  GROUP BY 1,2
  ORDER BY 1,2
;

                data_source                 | shape_leng units/meter | count
--------------------------------------------+------------------------+-------
 Spatial_Files/HydraulicDCS_HUC02010006.gdb |                   3.29 |   319
 Spatial_Files/HydraulicDCS_HUC04150301.gdb |                   3.28 |    79
 Spatial_Files/HydraulicDCS_HUC04150302.gdb |                   3.28 |   834
 Spatial_Files/HydraulicDCS_HUC04150303.gdb |                   3.28 |    12
 Spatial_Files/HydraulicDCS_HUC04150304.gdb |                   3.28 |  1112
 Spatial_Files/HydraulicDCS_HUC04150305.gdb |                   3.28 |  1006
 Spatial_Files/HydraulicDCS_HUC04150305.gdb |                   3.29 |    37
 Spatial_Files/HydraulicDCS_HUC04150306.gdb |                   3.28 |  1203
 Spatial_Files/HydraulicDCS_HUC04150306.gdb |                   3.29 |    33
 Spatial_Files/HydraulicDCS_HUC04150306.gdb |                        |     2
 Spatial_Files/HydraulicDCS_HUC04150307.gdb |                   3.28 |    82
 Spatial_Files/HydraulicDCS_HUC04150307.gdb |                   3.29 |   433
 Spatial_Files/HydraulicDCS_HUC04150308.gdb |                   3.29 |   167
(13 rows)

dama_dev_1=# select * from tmp_ble_fld_haz_ar_perimeters where perimeter_meters is null;
 fid  | perimeter_meters
------+------------------
 1752 |
 1761 |
(2 rows)

dama_dev_1=# select fld_ar_id, gfid from merged_ble_fld_haz_ar where fid = 1752;
   fld_ar_id   |                       gfid                       
---------------+--------------------------------------------------
 04150306_1071 | Spatial_Files/HydraulicDCS_HUC04150306.gdb::1071
(1 row)

dama_dev_1=# select fld_ar_id, gfid from merged_ble_fld_haz_ar where fid = 1761;
   fld_ar_id   |                       gfid                       
---------------+--------------------------------------------------
 04150306_1080 | Spatial_Files/HydraulicDCS_HUC04150306.gdb::1080
(1 row)

ogr2ogr -F GeoJSON -dialect sqlite -sql "select * from S_Fld_Haz_Ar where FLD_AR_ID = '04150306_1071'" /vsistdout/ HydraulicDCS_HUC04150306.gdb | jq
{
  "type": "FeatureCollection",
  "name": "SELECT",
  "crs": {
    "type": "name",
    "properties": {
      "name": "urn:ogc:def:crs:EPSG::4019"
    }
  },
  "features": [
    {
      "type": "Feature",
      "properties": {
        "DFIRM_ID": "NYBLE",
        "VERSION_ID": "1.1.1.1",
        "FLD_AR_ID": "04150306_1071",
        "STUDY_TYP": "1030",
        "FLD_ZONE": "X",
        "ZONE_SUBTY": "0500",
        "SFHA_TF": "F",
        "STATIC_BFE": null,
        "V_DATUM": null,
        "DEPTH": null,
        "LEN_UNIT": null,
        "VELOCITY": null,
        "VEL_UNIT": null,
        "AR_REVERT": null,
        "AR_SUBTRV": null,
        "BFE_REVERT": null,
        "DEP_REVERT": null,
        "DUAL_ZONE": null,
        "SOURCE_CIT": "STUDY1",
        "SHAPE_Length": 0,
        "SHAPE_Area": 0
      },
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [
          []
        ]
      }
    }
  ]
}

ogr2ogr -F GeoJSON -dialect sqlite -sql "select * from S_Fld_Haz_Ar where FLD_AR_ID = '04150306_1080'" /vsistdout/ HydraulicDCS_HUC04150306.gdb | jq
{
  "type": "FeatureCollection",
  "name": "SELECT",
  "crs": {
    "type": "name",
    "properties": {
      "name": "urn:ogc:def:crs:EPSG::4019"
    }
  },
  "features": [
    {
      "type": "Feature",
      "properties": {
        "DFIRM_ID": "NYBLE",
        "VERSION_ID": "1.1.1.1",
        "FLD_AR_ID": "04150306_1080",
        "STUDY_TYP": "1030",
        "FLD_ZONE": "X",
        "ZONE_SUBTY": "0500",
        "SFHA_TF": "F",
        "STATIC_BFE": null,
        "V_DATUM": null,
        "DEPTH": null,
        "LEN_UNIT": null,
        "VELOCITY": null,
        "VEL_UNIT": null,
        "AR_REVERT": null,
        "AR_SUBTRV": null,
        "BFE_REVERT": null,
        "DEP_REVERT": null,
        "DUAL_ZONE": null,
        "SOURCE_CIT": "STUDY1",
        "SHAPE_Length": 0,
        "SHAPE_Area": 0
      },
      "geometry": null
    }
  ]
}
```

