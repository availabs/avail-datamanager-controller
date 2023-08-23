# NYS Bridges Data Integrity Issues

## 1. NYSDOT Structures Bridges

The [NYSDOT Bridges](https://data.gis.ny.gov/datasets/9e038774ef034c7cae5374f3e23f7a67_0/about?layer=0)
GIS dataset is a layer in the
[NYSDOT Structures](https://data.gis.ny.gov/maps/9e038774ef034c7cae5374f3e23f7a67/about)
GIS dataset.

Documentation: [NYSDOT BRIDGE AND LARGE CULVERT INVENTORY MANUAL July 2020](https://www.dot.ny.gov/divisions/engineering/structures/repository/manuals/inventory/NYSDOT_inventory_manual_2020.pdf)

> ITEM: **Bridge Identification Number (BIN)**
>
> FHWA 8
>
> PROCEDURE:
>   A unique seven-character bridge identification number (BIN) assigned to each bridge.

### 1.1 Total number of bridges in the NYSDOT Structures Bridges Dataset: 19,980

```sql
SELECT
    COUNT(1)
  FROM nysdot_structures.bridges
;

 count 
-------
 19980
(1 row)
```

### 1.2 There are, however, a duplicate entries in the NYSDOT dataset:

From the [NYSDOT BRIDGE AND LARGE CULVERT INVENTORY MANUAL](https://www.dot.ny.gov/divisions/engineering/structures/repository/manuals/inventory/NYSDOT_inventory_manual_2020.pdf):


```sql
SELECT
    nysdot_bin,
    COUNT(1)
  FROM nysdot_structures.nysdot_usdot_bridges_condition
  GROUP BY nysdot_bin
  HAVING COUNT(1) > 1
  ORDER BY 2 DESC, 1
;

 nysdot_bin | count 
------------+-------
 1025040    |     2
 1091459    |     2
 3369100    |     2
 5524890    |     2
 5524900    |     2
(5 rows)
```

### 1.2.1 Further details on the duplicate bridge entries:

```sql
SELECT
    county,
    bin,
    carried,
    crossed,
    ST_X(wkb_geometry) AS longitude,
    ST_Y(wkb_geometry) AS latitude
  FROM nysdot_structures.bridges
  WHERE bin IN (
    SELECT DISTINCT
        nysdot_bin
      FROM nysdot_structures.nysdot_usdot_bridges_condition
      GROUP BY nysdot_bin
      HAVING COUNT(1) > 1
  )
  ORDER BY 1,2
;

   county    |   bin   |     carried      |      crossed      |     longitude     |     latitude     
-------------+---------+------------------+-------------------+-------------------+------------------
 DELAWARE    | 5524890 | DEC Access Rd    | Russell Brook     | -74.9826070519999 |     41.964378595
 DELAWARE    | 5524890 | DEC Access Rd    | Russell Brook     |     -74.941208799 | 41.9951489170001
 DELAWARE    | 5524900 | DEC Access Rd    | Mud Pond          |     -74.942122425 |     41.996219835
 DELAWARE    | 5524900 | DEC Access Rd    | Mud Pond          |     -74.976568228 |     41.968104408
 ROCKLAND    | 3369100 | POMONA ROAD      | MINISCEONGO CREEK |     -74.048836547 | 41.1672933350001
 ROCKLAND    | 3369100 | POMONA ROAD      | MINISCEONGO CREEK | -77.7282207279999 | 42.0232413300001
 SULLIVAN    | 1025040 | 55  55 96011378  | UNNAMED STREAM    | -74.5808933119999 | 41.8587083560001
 SULLIVAN    | 1025040 | 55  55 96011378  | UNNAMED STREAM    |     -74.580893096 | 41.8587066590001
 WESTCHESTER | 1091459 | 684I684I87011083 | BYRAM RIVER       | -73.7034269439999 |     41.132401739
 WESTCHESTER | 1091459 | 684I684I87011083 | BYRAM RIVER       |     -73.701355429 |     41.136764359
(10 rows)
```

###
![Duplicate Bridge Entries for 5524890 and 5524900](./images/NYSDOT_Structures_Bridges_Duplicates.1.png)
![Duplicate Bridge Entries for 5524890 and 5524900](./images/NYSDOT_Structures_Bridges_Duplicates.2.png)
![Duplicate Bridge Entries for 5524890 and 5524900](./images/NYSDOT_Structures_Bridges_Duplicates.3_1.png)
![Duplicate Bridge Entries for 5524890 and 5524900](./images/NYSDOT_Structures_Bridges_Duplicates.3_2.png)
![Duplicate Bridge Entries for 5524890 and 5524900](./images/NYSDOT_Structures_Bridges_Duplicates.3_3.png)

---
---

## 2. USDOT BTS National Bridges Inventory (NBI)

Documentation: [The Recording and Coding Guide for the Structure Inventory and Appraisal
of the Nation's Bridges](https://www.fhwa.dot.gov/bridge/mtguide.pdf).

> Item 8 - Structure Number 15 digits
>
> It is required that the official structure number be recorded. It is
> not necessary to code this number according to an arbitrary national
> standard. Each agency should code the structure number according to its
> own internal processing procedures. When recording and coding for this
> item and following items, any structure or structures with a closed
> median should be considered as one structure, not two. Closed medians
> may have either mountable or non-mountable curbs or barriers.
> The structure number must be unique for each bridge within the State,
> and once established should preferably never change for the life of the
> bridge. If it is essential that structure number(s) must be changed,
> all 15 digits are to be filled. For any structure number changes, a
> complete cross reference of corresponding "old" and "new" numbers must
> be provided to the FHWA Bridge Division. The cross reference shall
> include both a computer tape or diskette and a printed listing in the
> FHWA required format.

### 2.1 Total number of NY bridges in the NBI: 17,573

```sql
SELECT
    COUNT(1)
  FROM us_bureau_of_transportation_statistics.ny_bridge_inventory
;

 count 
-------
 17573
(1 row)
```

### 2.2 There are no duplicate structure IDs in the NBI for NYS:

```sql
SELECT
    structure,
    COUNT(1)
  FROM us_bureau_of_transportation_statistics.ny_bridge_inventory
  GROUP BY structure
  HAVING COUNT(1) > 1
;

 structure | count 
-----------+-------
(0 rows)
```

---
---

## 3. NYSDOT Structures Bridges AND USDOT BTS National Bridges Inventory (NBI)

NOTE: There are 19,975 unique bridge IDs in the NYSDOT dataset and 17,573 in
the NBI. This ia a difference of 2,407, or 12%, of the total number of unique
bridge IDs in the NYSDOT dataset.

However, the inconsistency grows upon closer inspection.

### 3.1 Total number of distinct bridge IDs found in both the USDOT and NYSDOT bridges datasets: 17,510

![Set Intersection Visualization](./images/SetIntersection.svg)

When a bridge ID is shared between the two datasets, we can join the information
from the two datasets for a fuller representation of the bridge.
(NOTE: this assumes a given ID accurately identifies the same physical structure.
This assumption requires spatial analysis to confirm its soundness.)

```sql
SELECT
    COUNT(1)
  FROM (
    SELECT
        TRIM(LEADING '0' FROM bin)
      FROM nysdot_structures.bridges
    INTERSECT
    SELECT
        TRIM(leading '0' from structure)
      FROM us_bureau_of_transportation_statistics.ny_bridge_inventory
  ) AS t
;

 count 
-------
  17510
(1 row)
```

### 3.2 Bridge IDs found in one, but not both, of the bridge datasets

![Set Difference Visualization](./images/SetDifference.svg)

When a bridge ID is not shared between the two datasets, the information
available to describe the associated structure is limited to
the respective dataset containing the ID.
(NOTE: It is possible to use spatial analysis to suggest instances
where the same physical structure is assigned two different IDs, but
this process us much more labor intensive and requires human confirmation of the results.)

#### 3.2.1 Total number of distinct NYSDOT bridge IDs not found in the USDOT bridges dataset: 2,465

```sql
SELECT
    COUNT(1)
  FROM (
    SELECT
        TRIM(LEADING '0' FROM bin)
      FROM nysdot_structures.bridges
    EXCEPT
    SELECT
        TRIM(leading '0' from structure)
      FROM us_bureau_of_transportation_statistics.ny_bridge_inventory
  ) AS t
;

 count 
-------
  2465
(1 row)
```

#### 3.2.2 Total number of distinct USDOT bridge IDs not found in the NYSDOT bridges dataset: 63

```sql
SELECT
    COUNT(1)
  FROM (
    SELECT
        TRIM(leading '0' from structure)
      FROM us_bureau_of_transportation_statistics.ny_bridge_inventory
    EXCEPT
    SELECT
        TRIM(LEADING '0' FROM bin)
      FROM nysdot_structures.bridges
  ) AS t
;

 count 
-------
  63
(1 row)
```

## 4 Bridges missing from both datasets

Using the [USGS National Hydrography Dataset (NHD)](https://www.sciencebase.gov/catalog/item/61f8b8e1d34e622189c32924) and the [OpenStreetMap](https://www.openstreetmap.org/about) road network, we can identify potential
bridges missing from both the NYSDOT and USDOT bridges datasets.

In the following images,
* Red: USDOT NBI Bridges
* Blue: NYSDOT Bridges
* Green: NYSDOT Culverts
* Black: OSM Bridges crossing US Geographic Survey NHD Flowlines
* Small Grey: OSM Roadways crossing US Geographic Survey NHD Flowlines

NOTE:
OSM Bridges are elements of the OSM Roadway network that are labeled as
[bridges](https://wiki.openstreetmap.org/wiki/Key:bridge) in the metadata.

From the [NHD documentation](https://www.usgs.gov/national-hydrography/national-hydrography-dataset):

> NHDFlowline is the fundamental flow network consisting predominantly of
> stream/river and artificial path vector features.

### 4.1 State-wide Bridges Map

![NYS Bridges Overview](./images/nys_bridges_resized/NYS_Bridges_Overview.all.resized.png)

*NYS Bridges from USDOT, NYSOT, OSM*

---

![NYS Bridges Overview](./images/nys_bridges_resized/NYS_Bridges_Overview.USDOT-only.resized.png)

*NYS Bridges from USDOT only*

---

![NYS Bridges Overview](./images/nys_bridges_resized/NYS_Bridges_Overview.NYSDOT-only.resized.png)

*NYS Bridges from NYSDOT only*

---

![NYS Bridges Overview](./images/nys_bridges_resized/NYS_Bridges_Overview.OSMBridge-x-NHDFlowline.resized.png)

*NYS Bridges from OSM Bridges crossing US Geographic Survey NHD Flowlines only*

### 4.2 Closeup Bridges Map

![NYS Bridges Closeup](./images/nys_bridges_resized/NYS_Bridges_Closeup.USDOT_and_NYSDOT.resized.png)

*NYS Bridges and Culverts from USDOT and NYSDOT only*

---

![NYS Bridges Closeup](./images/nys_bridges_resized/NYS_Bridges_Closeup.USDOT_and_NYSDOT_and_OSMBridges_x_NHDFlowlines.resized.png)

*NYS Bridges and Culverts from USDOT and NYSDOT and OSM Bridge crosses NHDFlowline*

---

![NYS Bridges Closeup](./images/nys_bridges_resized/NYS_Bridges_Closeup.USDOT_and_NYSDOT_and_OSMBridges_x_NHDFlowlines_and_OSMRoadways_x_NHDFlowlines.resized.png)

*NYS Bridges and Culverts from USDOT and NYSDOT and OSM Bridge crosses NHDFlowline and OSM Roadway crosses NHDFlowline*

## Bridges_and_Culverts_Mixed

Both the USDOT Bureau of Transportation Statistics National Bridge Inventory (NBI)
dataset and the NYSDOT Structures Bridges dataset include culverts.

From [The Recording and Coding Guide for the Structure Inventory and Appraisal of the Nation's Bridges](https://www.fhwa.dot.gov/bridge/mtguide.pdf):

> Item 62 - Culverts
> ...
> Item 58 - Deck, Item 59 - Superstructure, and Item 60 - Substructure
> shall be coded N for all culverts.

There are 2066 culverts that exists in both the NBI and NYSDOT Bridges datasets.

```sql
SELECT
    nysdot_gtms_struct,
    COUNT(1)
  FROM nysdot_structures.nysdot_usdot_bridges_join
  WHERE (
    ( usdot_deck_cond = 'N' )
    AND
    ( usdot_superstruc = 'N' )
    AND
    ( usdot_substructu = 'N' )
    AND
    ( nysdot_bin IS NOT NULL )
  )
  GROUP BY 1
;

 nysdot_gtms_struct | count 
--------------------+-------
 19 - Culvert       |  2066
(1 row)
```

Overall, there are 2125 culverts in the NYSDOT Bridges dataset.

```sql
SELECT
    COUNT(1)
  FROM nysdot_structures.bridges
  WHERE ( gtms_struct = '19 - Culvert' )
;

 count 
-------
  2125
(1 row)
```

```sql
SELECT
    bin
  FROM nysdot_structures.culverts
  WHERE ( bin NOT ILIKE 'C%' )
;
 bin 
-----
(0 rows)
```

```sql
SELECT
    *
  FROM nysdot_structures.bridges AS a
    INNER JOIN nysdot_structures.culverts AS b
      ON (('C' || a.bin) = UPPER(b.bin))
  LIMIT 10
;
(0 rows)
```

A visual inspection of the NYSDOT bridges and culverts suggests some culverts
may appear in both datasets. The number of such cases is likely less than 100,
however further work is required to provide an estimate with any confidence.

