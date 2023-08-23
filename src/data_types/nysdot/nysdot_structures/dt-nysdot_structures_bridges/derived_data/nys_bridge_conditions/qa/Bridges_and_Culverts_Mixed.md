# Bridges_and_Culverts_Mixed

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
but further analysis is required to say for certain.

