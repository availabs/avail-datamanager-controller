# nysdot\_culverts\_in\_poor\_condition\_aggregated

## Description

The nysdot\_culverts\_in\_poor\_condition\_aggregated CSV contains
aggregated information from the
[NYSDOT Structures Large Culverts](https://data.gis.ny.gov/maps/9e038774ef034c7cae5374f3e23f7a67)
dataset (NYSDOT Culverts).

The CSV aggregates, by county and culvert agency owner class,
the total number of culverts that meet certain criteria.

For each county and agency class, the following sums are provided:

* Total culverts (**total_culverts**)
* Total culverts in poor condition (**total_culverts_poor_condition**)
* Total culverts with condition unknown (**total_culverts_unk_condition**)
* Total culverts that channel water (**xwater_total_culverts**)
* Total culverts in poor condition that channel water (**xwater_total_culverts_poor_condition**)
* Total culverts in unknown condition that channel water (**xwater_total_culverts_unk_condition**)
* Total culverts that may or may not channel water (**xwater_unk_total_culverts**)
* Total culverts in poor condition that may or may not channel water (**xwater_unk_total_culverts_poor_condition**)
* Total culverts in unknown condition that may or may not channel water (**xwater_unk_total_culverts_unk_condition**)


### Culvert Owner Agency Class

The culvert owner agency classes are:

* Federal
* State
* County
* Municipal
* Authority or Commission
* Railroad
* Other

### Culvert Condition

A culvert is determined to be in poor condition if the condition rating is less
than 5.

NOTE: In some cases, a culvert's condition is unknown because the condition
rating field is empty in the NYSDOT Culverts dataset. These cases are tallied
under "unk\_condition".

A condition rating score below 5, means that the culvert's condition is worse
than "Minor deterioration but functioning as originally designed." 

From the [Culvert Inventory and Inspection
Manual](https://www.dot.ny.gov/divisions/operating/oom/transportation-maintenance/repository/CulvertInventoryInspectionManual.pdf)).

> ITEM: General Recommendation
> This is a noneditable field and the value is populated from the last approved
> General Inspection of the structure. The structure is given an overall
> assessment using the codes below. Please see Section 6 of the NYSDOT Bridge
> Inspection Manual for more information.

| rating | description
|---|---
| 7 | New condition. No deterioration.
| 6 | Used to shade between ratings of 5 and 7.
| 5 | Minor deterioration but functioning as originally designed.
| 4 | Used to shade between ratings of 3 and 5.
| 3 | Serious deterioration, or not functioning as originally designed.
| 2 | Used to shade between ratings of 1 and 3.
| 1 | Totally deteriorated, or in failed condition.

<br> 

The [NYSDOT Culvert Inspection Field Guide](https://www.dot.ny.gov/divisions/operating/oom/transportation-maintenance/repository/CULVERT%20INSPECTION%20FIELD%20GUIDE%201-18-06.pdf) provides further explanation
of culvert condition ratings:

> **General Recommendation Descriptions for Entire Structure**
> 
> The General Recommendation is the rating given to the culvert as a whole.
> Keep in mind that the individual item ratings of more important culvert elements, such as
> those in the Structure Items section, should have a greater influence in determining the
> General Recommendation than individual item ratings of less important culvert elements
> such as those in the Roadway Items and Channel Items sections.
> It is important that the General Recommendation rating not be lower than the lowest
> rating given to any individual item.
> The General Recommendation rating does not have to match the lowest rating given for
> any individual item unless that item is of major consequence in comparison to the other
> items.
> In addition to considering the relative importance of the items to determine the General
> Recommendation, consult the following narrative descriptions:

> | rating | description |
> |--------|-------------|
> | 7 | Like new condition. No repairs required.
> | 6 | May require very minor repairs to pavement, guiderail, shoulders, etc.
> | 5 | May require minor repairs to the headwalls or wingwalls. May require removal of light vegetation growth around culvert openings.
> | 4 | Pavement may require replacement with the addition of backfill material to correct minor roadway settlement problems yet the structure shows no signs of deformation or settlement. Wingwalls and headwalls may require significant repair work. Some minor work to the channel may be required.
> | 3 | Significant repairs to the pavement are required due to settlement. Slight deformation and settlement of the structure exists. Significant deterioration of wingwalls and/or headwalls exists. Extensive work on the culvert is required. Replacement could be considered a better long term option.
> | 2 | Replacement of the structure is necessary due to serious deformation and settlement of the structure. Short-term, remedial action such as pavement replacement or installation of additional backfill material is required. Temporary shoring may be needed or already exist. A vehicle load restriction is probably posted. Replacement of wingwalls and/or headwalls is required. Alignment of waterway is such that significant, measurable and progressive, general and /or localized scour is occurring. Constriction or obstruction of the culvert opening greatly restricts water flow.
> | 1 | Pavement has settled as a result of significant structure deformation or settlement. Structure has collapsed or collapse is likely. Culvert opening is closed or nearly closed due to embankment soil failure, structure deformation, channel sedimentation, debris accumulation, or vegetation growth. Roadway should have traffic restrictions or be closed to traffic entirely. 

## Culvert Channels Water (xwater)

A culvert is determined to channel water if the NYSDOT Culverts Dataset
stream_bed_material field is *NOT* "1 - No Waterway".

NOTE: Cases where the stream_bed_material field is empty are tallied under "xwater_unk".

From the [NYSDOT Culvert Inventory and Inspection Manual](https://www.dot.ny.gov/divisions/operating/oom/transportation-maintenance/repository/CulvertInventoryInspectionManual.pdf):

> INVENTORY ITEM: Stream Bed Material
>
> Record the most predominant stream bed material in the area of the culvert.

| Code | Description |
|---|---|
| 0 | Other
| 1 | No Waterway
| 2 | Bed Rock
| 3 | Large Stone
| 4 | Gravel
| 5 | Sand
| 6 | Silt
| 7 | Clay

<br>

## Schema

Combining culvert owner class, culvert condition, and culvert channels water,
we get the folloing columns for the CSV:

* county
<br> 

* total_culverts_federal_owned
* total_culverts_poor_condition_federal_owned
* total_culverts_unk_condition_federal_owned
* xwater_total_culverts_federal_owned
* xwater_total_culverts_poor_condition_federal_owned
* xwater_total_culverts_unk_condition_federal_owned
* xwater_unk_total_culverts_federal_owned
* xwater_unk_total_culverts_poor_condition_federal_owned
* xwater_unk_total_culverts_unk_condition_federal_owned
<br> 

* total_culverts_state_owned
* total_culverts_poor_condition_state_owned
* total_culverts_unk_condition_state_owned
* xwater_total_culverts_state_owned
* xwater_total_culverts_poor_condition_state_owned
* xwater_total_culverts_unk_condition_state_owned
* xwater_unk_total_culverts_state_owned
* xwater_unk_total_culverts_poor_condition_state_owned
* xwater_unk_total_culverts_unk_condition_state_owned
<br> 

* total_culverts_county_owned
* total_culverts_poor_condition_county_owned
* total_culverts_unk_condition_county_owned
* xwater_total_culverts_county_owned
* xwater_total_culverts_poor_condition_county_owned
* xwater_total_culverts_unk_condition_county_owned
* xwater_unk_total_culverts_county_owned
* xwater_unk_total_culverts_poor_condition_county_owned
* xwater_unk_total_culverts_unk_condition_county_owned
<br> 

* total_culverts_municipality_owned
* total_culverts_poor_condition_municipality_owned
* total_culverts_unk_condition_municipality_owned
* xwater_total_culverts_municipality_owned
* xwater_total_culverts_poor_condition_municipality_owned
* xwater_total_culverts_unk_condition_municipality_owned
* xwater_unk_total_culverts_municipality_owned
* xwater_unk_total_culverts_poor_condition_municipality_owned
* xwater_unk_total_culverts_unk_condition_municipality_ow
<br> 

* total_culverts_auth_or_comm_owned
* total_culverts_poor_condition_auth_or_comm_owned
* total_culverts_unk_condition_auth_or_comm_owned
* xwater_total_culverts_auth_or_comm_owned
* xwater_total_culverts_poor_condition_auth_or_comm_owned
* xwater_total_culverts_unk_condition_auth_or_comm_owned
* xwater_unk_total_culverts_auth_or_comm_owned
* xwater_unk_total_culverts_poor_condition_auth_or_comm_owned
* xwater_unk_total_culverts_unk_condition_auth_or_comm_ow
<br> 

* total_culverts_railroad_owned
* total_culverts_poor_condition_railroad_owned
* total_culverts_unk_condition_railroad_owned
* xwater_total_culverts_railroad_owned
* xwater_total_culverts_poor_condition_railroad_owned
* xwater_total_culverts_unk_condition_railroad_owned
* xwater_unk_total_culverts_railroad_owned
* xwater_unk_total_culverts_poor_condition_railroad_owned
* xwater_unk_total_culverts_unk_condition_railroad_owned
<br> 

* total_culverts_other_owned
* total_culverts_poor_condition_other_owned
* total_culverts_unk_condition_other_owned
* xwater_total_culverts_other_owned
* xwater_total_culverts_poor_condition_other_owned
* xwater_total_culverts_unk_condition_other_owned
* xwater_unk_total_culverts_other_owned
* xwater_unk_total_culverts_poor_condition_other_owned
* xwater_unk_total_culverts_unk_condition_other_owned

## Resources

### NYSDOT Structures Large Culverts Dataset

source: [NYSDOT Structures Large Culverts](https://data.gis.ny.gov/maps/9e038774ef034c7cae5374f3e23f7a67)

official documentation:

* [NYSDOT Culvert Inventory and Inspection Manual May 2006](dot.ny.gov/divisions/operating/oom/transportation-maintenance/repository/CulvertInventoryInspectionManual.pdf)
* [NYSDOT BRIDGE AND LARGE CULVERT INVENTORY MANUAL July 2020](https://www.dot.ny.gov/divisions/engineering/structures/repository/manuals/inventory/NYSDOT\_inventory\_manual\_2020.pdf)
* [NYSDOT Culvert Inspection Field Guide](https://www.dot.ny.gov/divisions/operating/oom/transportation-maintenance/repository/CULVERT%20INSPECTION%20FIELD%20GUIDE%201-18-06.pdf)
* [Culverts: Inspection, Failure Modes & Rehabilitation](https://www.dot.ny.gov/divisions/engineering/structures/repository/events-news/2019_LBC_session_3-1.pdf)

## Issues

The NYSDOT Structures Large Culverts is sparse. There are many places where the
road network crosses flowing water that are not represented in the NYSDOT
Bridges and Culverts datasets.

Additionally, a small portion of culverts also appear in the NYSDOT Structures
Bridges dataset and are currently double counted. Reliably identifying these
culverts would require additional work.
