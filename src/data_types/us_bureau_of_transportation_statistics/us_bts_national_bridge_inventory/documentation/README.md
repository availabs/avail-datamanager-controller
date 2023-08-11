
🔑🔑🔑 [Detailed Code Mapping for Individual Data Items](https://www.fhwa.dot.gov/bridge/snbi/codemapping.cfm)

[Data Crosswalk for “Over” Records and “Under” Records](https://www.fhwa.dot.gov/bridge/snbi/datacrosswalk.cfm)

[National Bridge Inventory [NBI] - Data Dictionary](https://nationalbridges.com/nbiDesc.html)

[NYSDOT_inventory_manual_2020](https://www.dot.ny.gov/divisions/engineering/structures/repository/manuals/inventory/NYSDOT_inventory_manual_2020.pdf)

[National Bridge Inventory (NBI) – Based on the SNBI](https://www.fhwa.dot.gov/bridge/snbi.cfm)

[Feature Type, when are more than one waterway features reported (and all associated items for that feature) and when are both a waterway feature and relief for waterway feature reported?](https://www.fhwa.dot.gov/bridge/snbi/qanda/06.cfm):

> AS4F-1 A W## - waterway or a F## - relief for waterway feature type should be
> reported when a bridge spans over water. This applies whether the water
> source is perennial or intermittent, including if flow exists only during
> flood stage. A W## feature type is reported for each unique waterway spanned
> by the bridge. A bridge that spans a single unique waterway and/or its
> associated floodplain, will have a single feature reported. Also, a bridge
> that spans multiple channels or branches of the same unique waterway will
> have a single feature reported. A bridge that spans more than one unique
> waterway, although very rare, will have multiple features reported (i.e.,
> W01, W02).
>
> A F## feature type is reported for relief bridges that provide waterway opening
> for flow only during flood stages and furnish additional hydraulic capacity.
>
> A W## and separate F## feature type would be reported only when the bridge
> spans a waterway and a separate relief channel is constructed in the
> floodplain; this would be a very rare condition.

## Possible cause for on/off ramps getting own row in the database.

From The Recording and Coding Guide for the Structure Inventory and Appraisal
of the Nation's Bridges [Item 8 - Structure Number](https://www.fhwa.dot.gov/bridge/mtguide.pdf#page=17):

> any structure or structures with a closed median should be considered as one structure, not two.

It looks like bridges are counted twice if they have an open median.

## Full Column Names

ESRI Shapefiles truncate column names.

These are the full column names. -- source [Hawaii Statewide GIS Program](https://geoportal.hawaii.gov/datasets/HiStateGIS::national-bridge-inventory/about).

NOTE: the numeric suffixes are the ItemNumber in the
[Recording and Coding Guide for the Structure Inventory and Appraisal of the Nation's Bridges](
https://www.fhwa.dot.gov/bridge/mtguide.pdf
).

```
STATE_CODE_001
STRUCTURE_NUMBER_008
RECORD_TYPE_005A
ROUTE_PREFIX_005B
SERVICE_LEVEL_005C
ROUTE_NUMBER_005D
DIRECTION_005E
HIGHWAY_DISTRICT_002
COUNTY_CODE_003
PLACE_CODE_004
FEATURES_DESC_006A
FACILITY_CARRIED_007
LOCATION_009
MIN_VERT_CLR_010
KILOPOINT_011
BASE_HWY_NETWORK_012
LRS_INV_ROUTE_013A
SUBROUTE_NO_013B
LAT_016
Lat_Degree
Lat_Minute
Lat_Second
Lat_DD
LONG_017
Long_Degree
Long_Minute
Long_Second
Long_DD
DETOUR_KILOS_019
TOLL_020
MAINTENANCE_021
OWNER_022
FUNCTIONAL_CLASS_026
YEAR_BUILT_027
TRAFFIC_LANES_ON_028A
TRAFFIC_LANES_UND_028B
ADT_029
YEAR_ADT_030
DESIGN_LOAD_031
APPR_WIDTH_MT_032
MEDIAN_CODE_033
DEGREES_SKEW_034
STRUCTURE_FLARED_035
RAILINGS_036A
TRANSITIONS_036B
APPR_RAIL_036C
APPR_RAIL_END_036D
HISTORY_037
NAVIGATION_038
NAV_VERT_CLR_MT_039
NAV_HORR_CLR_MT_040
OPEN_CLOSED_POSTED_041
SERVICE_ON_042A
SERVICE_UND_042B
STRUCTURE_KIND_043A
STRUCTURE_TYPE_043B
APPR_KIND_044A
APPR_TYPE_044B
MAIN_UNIT_SPANS_045
APPR_SPANS_046
HORR_CLR_MT_047
MAX_SPAN_LEN_MT_048
STRUCTURE_LEN_MT_049
LEFT_CURB_MT_050A
RIGHT_CURB_MT_050B
ROADWAY_WIDTH_MT_051
DECK_WIDTH_MT_052
VERT_CLR_OVER_MT_053
VERT_CLR_UND_REF_054A
VERT_CLR_UND_054B
LAT_UND_REF_055A
LAT_UND_MT_055B
LEFT_LAT_UND_MT_056
DECK_COND_058
SUPERSTRUCTURE_COND_059
SUBSTRUCTURE_COND_060
CHANNEL_COND_061
CULVERT_COND_062
OPR_RATING_METH_063
OPERATING_RATING_064
INV_RATING_METH_065
INVENTORY_RATING_066
STRUCTURAL_EVAL_067
DECK_GEOMETRY_EVAL_068
UNDCLRENCE_EVAL_069
POSTING_EVAL_070
WATERWAY_EVAL_071
APPR_ROAD_EVAL_072
WORK_PROPOSED_075A
WORK_DONE_BY_075B
IMP_LEN_MT_076
DATE_OF_INSPECT_090
INSPECT_FREQ_MONTHS_091
FRACTURE_092A
UNDWATER_LOOK_SEE_092B
SPEC_INSPECT_092C
FRACTURE_LAST_DATE_093A
UNDWATER_LAST_DATE_093B
SPEC_LAST_DATE_093C
BRIDGE_IMP_COST_094
ROADWAY_IMP_COST_095
TOTAL_IMP_COST_096
YEAR_OF_IMP_097
OTHER_STATE_PCNT_098B
OTHR_STATE_STRUC_NO_099
STRAHNET_HIGHWAY_100
PARALLEL_STRUCTURE_101
TRAFFIC_DIRECTION_102
TEMP_STRUCTURE_103
HIGHWAY_SYSTEM_104
FEDERAL_LANDS_105
YEAR_RECONSTRUCTED_106
DECK_STRUCTURE_TYPE_107
SURFACE_TYPE_108A
MEMBRANE_TYPE_108B
DECK_PROTECTION_108C
PERCENT_ADT_TRUCK_109
NATIONAL_NETWORK_110
PIER_PROTECTION_111
BRIDGE_LEN_IND_112
SCOUR_CRITICAL_113
FUTURE_ADT_114
YEAR_OF_FUTURE_ADT_115
MIN_NAV_CLR_MT_116
FED_AGENCY
SUBMITTED_BY
BRIDGE_CONDITION
LOWEST_RATING
DECK_AREA
```
