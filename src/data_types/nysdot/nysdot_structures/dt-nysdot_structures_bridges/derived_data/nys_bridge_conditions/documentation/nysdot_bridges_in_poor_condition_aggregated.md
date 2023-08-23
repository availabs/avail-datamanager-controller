# nysdot\_bridges\_in\_poor\_condition\_aggregated

The nysdot\_bridges\_in\_poor\_condition\_aggregated CSV contains
aggregated information about NYS bridges.

The CSV aggregates, by county and bridge agency owner class,
the total number of bridges that meet certain criteria.

NOTE: While performing the aggregation, the query prefers the
county and bridge's primary owner as found in the NYSDOT dataset.
If there is no entry for the bridge in the NYSDOT dataset,
the query falls back to the county and owner found in ththe USDOT dataset.

The bridge owner agency classes are:

* Federal
* State
* County
* Municipal
* Authority or Commission
* Railroad
* Other

For each county and agency class, the following sums are provided:

* Total bridges (**total\_bridges**).

* Total bridges in poor condition (**total\_bridges\_poor\_condition**).

  A bridge is determined to be in poor condition if either:

  * According to the NYSDOT Structures Bridges dataset, the bridge's condition is
    worse than "Minor deterioration but functioning as originally designed."
  * According to the USDOT Bureau of Transportation Statistics National Bridges
    Inventory, if any of the deck, superstructure, substructure, channel or
    channel protection, or culverts are in poor condition or worse.

* Total bridges that cross water (**xwater\_total\_bridges**)

  A bridge is determined to cross water if the NBI SERVICE\_UNDER\_042B field
  is any of the following:
  * 5:  Waterway
  * 6:  Highway-waterway
  * 7:  Railroad-waterway
  * 8:  Highway-waterway-railroad
  * 9:  Relief for waterway

* Total bridges that cross water and are in poor condition (**xwater\_total\_bridges\_poor\_condition**)

* Total bridges that may or may not cross water (**xwater\_unknown\_total\_bridges**)

  Only the USDOT NBI dataset contains a field that explicitly states whether
  the bridge crosses water. If there is no entry for the bridge in the NBI,
  further work is required to determine whether the bridge crosses water.

* Total bridges that may or may not cross water are in poor condition
  (**xwater\_unknown\_total\_bridges\_poor\_condition**)

  See above point for explanation.

Combining all of the above, the CSV contains the following columns:

* county

* total\_bridges\_federal\_owned
* total\_bridges\_poor\_condition\_federal\_owned
* xwater\_total\_bridges\_federal\_owned
* xwater\_total\_bridges\_poor\_condition\_federal\_owned
* xwater\_unknown\_total\_bridges\_federal\_owned
* xwater\_unknown\_total\_bridges\_poor\_condition\_federal\_owned

* total\_bridges\_state\_owned
* total\_bridges\_poor\_condition\_state\_owned
* xwater\_total\_bridges\_state\_owned
* xwater\_total\_bridges\_poor\_condition\_state\_owned
* xwater\_unknown\_total\_bridges\_state\_owned
* xwater\_unknown\_total\_bridges\_poor\_condition\_state\_owned

* total\_bridges\_county\_owned
* total\_bridges\_poor\_condition\_county\_owned
* xwater\_total\_bridges\_county\_owned
* xwater\_total\_bridges\_poor\_condition\_county\_owned
* xwater\_unknown\_total\_bridges\_county\_owned
* xwater\_unknown\_total\_bridges\_poor\_condition\_county\_owned

* total\_bridges\_municipality\_owned
* total\_bridges\_poor\_condition\_municipality\_owned
* xwater\_total\_bridges\_municipality\_owned
* xwater\_total\_bridges\_poor\_condition\_municipality\_owned
* xwater\_unknown\_total\_bridges\_municipality\_owned
* xwater\_unknown\_total\_bridges\_poor\_condition\_municipality\_owned

* total\_bridges\_auth\_or\_comm\_owned
* total\_bridges\_poor\_condition\_auth\_or\_comm\_owned
* xwater\_total\_bridges\_auth\_or\_comm\_owned
* xwater\_total\_bridges\_poor\_condition\_auth\_or\_comm\_owned
* xwater\_unknown\_total\_bridges\_auth\_or\_comm\_owned
* xwater\_unknown\_total\_bridges\_poor\_condition\_auth\_or\_comm\_owned

* total\_bridges\_railroad\_owned
* total\_bridges\_poor\_condition\_railroad\_owned
* xwater\_total\_bridges\_railroad\_owned
* xwater\_total\_bridges\_poor\_condition\_railroad\_owned
* xwater\_unknown\_total\_bridges\_railroad\_owned
* xwater\_unknown\_total\_bridges\_poor\_condition\_railroad\_owned

* total\_bridges\_other\_owned
* total\_bridges\_poor\_condition\_other\_owned
* xwater\_total\_bridges\_other\_owned
* xwater\_total\_bridges\_poor\_condition\_other\_owned
* xwater\_unknown\_total\_bridges\_other\_owned
* xwater\_unknown\_total\_bridges\_poor\_condition\_other\_owned

## Resources

### USDOT Bureau of Transportation Statistics National Bridge Inventory

* source: [National Bridge Inventory](https://geodata.bts.gov/datasets/national-bridge-inventory/about)
* documenation: [The Recording and Coding Guide for the Structure Inventory and Appraisal of the Nation's Bridges](https://www.fhwa.dot.gov/bridge/mtguide.pdf)

### NYSDOT Bridges Dataset

* source: [NYSDOT Structures Bridges](https://data.gis.ny.gov/datasets/9e038774ef034c7cae5374f3e23f7a67_0/about?layer=0)
* documentation: [NYSDOT BRIDGE AND LARGE CULVERT INVENTORY MANUAL July 2020](https://www.dot.ny.gov/divisions/engineering/structures/repository/manuals/inventory/NYSDOT\_inventory\_manual\_2020.pdf)


## ISSUES

See the NYS_Bridges_Data_Integrity_Issues report.
