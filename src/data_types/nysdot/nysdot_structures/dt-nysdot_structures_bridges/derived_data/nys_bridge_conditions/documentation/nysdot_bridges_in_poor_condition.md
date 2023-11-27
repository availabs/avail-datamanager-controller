# nysdot\_bridges\_in\_poor\_condition

The nysdot\_bridges\_in\_poor\_condition CSV contains information about
the bridges in NYS that are in poor condition.

A bridge is determined to be in poor condition if either:

* According to the NYSDOT Structures Bridges dataset, the bridge's condition is
  worse than "Minor deterioration but functioning as originally designed."
* According to the USDOT Bureau of Transportation Statistics National Bridges
  Inventory, if any of the deck, superstructure, substructure, channel or
  channel protection, or culverts are in poor condition or worse.

## CSV Column Descriptions

NOTE: There are data integrity issues between the USDOT and the NYSDOT bridge
datasets. The nysdot\_bridges\_in\_poor\_condition CSV contains columns
repeated between the two datasets (such as AADT, latitude, and longitude) to
indicate where there may be discrepancies. For further analysis, see the
[NYS_Bridges_Data_Integrity_Issues](../qa/NYS_Bridges_Data_Integrity_Issues.md)
report.

### nysdot\_bin

> ITEM: Bridge Identification Number (BIN)
> FHWA 8
> PROCEDURE:
> A unique seven-character bridge identification number (BIN)

### usdot\_structure

> Item 8 - Structure Number 15 digits
>
> It is required that the official structure number be recorded. It is not
> necessary to code this number according to an arbitrary national standard.
> Each agency should code the structure number according to its own internal
> processing procedures. When recording and coding for this item and following
> items, any structure or structures with a closed median should be considered
> as one structure, not two. Closed medians may have either mountable or
> non-mountable curbs or barriers. The structure number must be unique for each
> bridge within the State, and once established should preferably never change
> for the life of the bridge.

### nysdot\_county

County.

### nysdot\_aadt

> ITEM: Average Daily Traffic (ADT)
>
> This field is noneditable. It is populated with a system job that imports
> this information from the Roadway Information System (RIS). This is populated
> for features that carry highway traffic. For bridges in parallel
> configuration and for those that carry one-way traffic only, the ADT should
> only reflect one direction of travel. For closed bridges, the last ADT entry
> will be retained. If the Feature is not a highway, this Item will be "0”.

### nysdot\_primary\_owner\_class

AVAIL added field: classification of the agency listed in the nysdot\_primary\_owner field.

|                                name                                |     classification
|--------------------------------------------------------------------|-------------------------
| 50 - Federal (Other than those listed below)                       | Federal
| 56 - Military Reservation / Corps of Engineers                     | Federal
| 53 - National Park Service                                         | Federal
| 20 - State - Other                                                 | State
| 22 - Alleghany State Park Authority                                | State
| 25 - Capital District State Park Commission                        | State
| 2L - NYS Thruway Authority                                         | State
| NYSDOT                                                             | State
| 2J - Niagara Frontier State Park Commission                        | State
| 2C - NYS Dept of Environmental Conservation                        | State
| 2P - NYS Power Authority                                           | State
| 26 - Central NY State Park Commission                              | State
| 2F - Long Island State Parks And Recreation Commission             | State
| 2K - NYS Bridge Authority                                          | State
| 2A - Genesee State Parks and Recreation Commission                 | State
| 27 - City of NY State Park Commission                              | State
| 23 - Nassau County Bridge Authority                                | County
| 30 - County                                                        | County
| 2H - Monroe County Water Authority                                 | County
| 42 - City                                                          | Municipal
| 41 - Village                                                       | Municipal
| 40 - Town                                                          | Municipal
| 43 - NYC Dept of Water Supply, Gas and Electric                    | Municipal
| 21 - Authority or Commission - Other                               | Authority or Commission
| 2N - Palisades Interstate Park Commission                          | Authority or Commission
| 2U - MTA Bridges and Tunnels (aka TBTA)                            | Authority or Commission
| 2T - Transit Authority                                             | Authority or Commission
| 2W - Port Authority of NY and NJ                                   | Authority or Commission
| 29 - Finger Lakes Parks and Recreation Commission                  | Authority or Commission
| 24 - Peace Bridge Authority (aka Buffalo And Ft. Erie Pub Br.Auth) | Authority or Commission
| 2G - Metropolitan Transportation Authority                         | Authority or Commission
| 2M - Ogdensburg Bridge and Port Authority                          | Authority or Commission
| 2I - Niagara Falls Bridge Commission                               | Authority or Commission
| 2B - Interstate Bridge Commission                                  | Authority or Commission
| 2Q - Seaway International Bridge Authority                         | Authority or Commission
| 2S - Thousand Islands Bridge Authority                             | Authority or Commission
| 72 - Other                                                         | Other
| 70 - Private - Industrial                                          | Other
| 71 - Private - Utility                                             | Other
| 62 - Retired (use to be Conrail - converted to 60)                 | Other
| 60 - Railroad                                                      | Railroad
| 61 - Long Island Railroad                                          | Railroad
| 50--50 - Federal (Other than those listed below)                   | Federal
| 56--56 - Military Reservation / Corps of Engineers                 | Federal
| 20--20 - State - Other                                             | State
| 10--NYSDOT                                                         | State
| 2J--2J - Niagara Frontier State Park Commission                    | State
| 2C--2C - NYS Dept of Environmental Conservation                    | State
| 2F--2F - Long Island State Parks And Recreation Commission         | State
| 2A--2A - Genesee State Parks and Recreation Commission             | State
| 30--30 - County                                                    | County
| 42--42 - City                                                      | Municipal
| 41--41 - Village                                                   | Municipal
| 40--40 - Town                                                      | Municipal
| 21--21 - Authority or Commission - Other                           | Authority or Commission
| 72--72 - Other                                                     | Other
| 70--70 - Private - Industrial                                      | Other

### nysdot\_primary\_owner

> ITEM: Owner (Primary and Secondary)
>
> Record the agency or agencies responsible for the non-federal share of the
> cost of replacement or rehabilitation of the bridge. Ownership of a bridge
> cannot normally be proven by the existence of a deed or some other form of
> title. Therefore, the owner shall be assumed to be the agency or agencies
> identified as having maintenance responsibility for the bridge. A different
> agency should be recorded as Owner only if there is tangible evidence, such
> as legislation or a written agreement indicating that the above assumptions
> are incorrect. The agencies responsible for Ownership and Maintenance
> Responsibility are identified on the Contract Plans when capital improvements
> are made.

### nysdot\_primary\_maintenance

> Item: Maintenance Responsibility (Primary and Secondary)
>
> Record the agency or agencies responsible for maintaining the bridge. This
> responsibility is established by law, agreement, or common consent. If
> tangible evidence is not available to establish this responsibility, it shall
> be assumed that the agencies that currently perform these activities are the
> responsible agencies. Ownership and Maintenance Responsibility are identified
> on the Contract Plans when capital improvements are made. The Highway Design
> Manual describes how maintenance responsibilities are assigned by law.

### nysdot\_condition\_rating

> ITEM: Condition Rating
>
> This is a noneditable field. The definition of Condition Rating is provided
> for historic context. The values in the system are a “Translated Condition
> Rating” based on the “Translated” AASHTO Elements to NYSDOT Elements
> effective March 2016. Condition Rating is a value calculated by NYSDOT to
> represent an overall assessment of the condition of a bridge. It is a
> numerical value from 1.000 (poor) - 7.000 (excellent).
>
> The computation uses 13 bridge elements considered most important for an
> overall condition appraisal. Each element is weighted in proportion to its
> relative importance. The condition of each element is multiplied by the
> assigned weight for that element, with the result divided by the sum of the
> weighted values, resulting in the Condition Rating for the bridge.
>
> The 13 bridge elements and their respective weights, are as follows:
>
> * Primary Members 10
> * Backwalls 5
> * Abutments (Stem/Breast wall) 8
> * Secondary Members 5
> * Piers 8
> * Joints 4
> * Structural Deck 8
> * Wearing Surface 4
> * Bridge Seats 6
> * Sidewalks 2
> * Bearings 6
> * Curbs 1
> * Wingwalls 5
>
> When a bridge has several elements of one kind, such as multiple piers, the
> lowest rating of all elements is used. Details of the relationship of
> condition values to the various elements used in the Structural Condition
> Formula may be found in the Department's Bridge Inspection Manual.
>
> Condition Rating is computed to three decimal places using the ratings of the
> thirteen elements with whole number values assigned. The three decimal point
> accuracy is significant only for the purpose of "breaking ties" when listing
> bridges by rank order of Condition Rating.

 CODING:
> 1 - Totally deteriorated, or in failed condition.
> 2 - Used to shade between ratings of 1 and 3.
> 3 - Serious deterioration, or not functioning as originally designed.
> 4 - Used to shade between ratings of 3 and 5.
> 5 - Minor deterioration but functioning as originally designed.
> 6 - Used to shade between ratings of 5 and 7.
> 7 - New condition. No deterioration.
> 8 - Not applicable.
> 9 - Condition and/or existence unknown.

### nysdot\_crossed

> ITEM: Description – or – Route Number and Reference Marker
>
> This item is used as the general description of the Feature Crossed. It is
> used to populate the Inspection Report and all other locations that refer to
> the “Feature Crossed” name as well as the SUBSETS table “Crossed” field.

### nysdot\_longitude

### nysdot\_latitude

### nysdot\_is\_in\_poor\_condition

AVAIL added field: true if nysdot\_condition\_rating < 5.

### usdot\_location\_009

> Item 9 - Location
>
> This item contains a narrative description of the bridge location. It is
> recommended that the location be keyed to a distinguishable feature on an
> official highway department map such as road junctions and topographical
> features. This item shall be left justified without trailing zeros.

### usdot\_adt\_029

> Item 29 - Average Daily Traffic 6 digits
>
> Code a 6-digit number that shows the average daily traffic volume for
> the inventory route identified in Item 5.
>
> The ADT coded should be the most recent ADT counts available.
> Included in this item are the trucks referred to in Item 109 - Average
> Daily Truck Traffic. If the bridge is closed, code the actual ADT from
> before the closure occurred.
>
> The ADT must be compatible with the other items coded for the bridge.
> For example, parallel bridges with an open median are coded as follows:
> if Item 28 - Lanes On and Under the Structure and Item 51 - Bridge
> Roadway Width, Curb-to-Curb are coded for each bridge separately, then
> the ADT must be coded for each bridge separately (not the total ADT for
> the route).

### usdot\_year\_adt\_030

> Item 30 - Year of Average Daily Traffic 4 digits
>
> Record the year represented by the ADT in Item 29. Code all four digits of
> the year so recorded.

### usdot_detour_km_019

> Item 19 - Bypass, Detour Length (XXX kilometers) 3 digits
> Indicate the actual length to the nearest kilometer of the detour
> length. The detour length should represent the total additional travel
> for a vehicle which would result from closing of the bridge. The factor
> to consider when determining if a bypass is available at the site is the
> potential for moving vehicles, including military vehicles, around the
> structure. This is particularly true when the structure is in an
> interchange. For instance, a bypass likely would be available in the
> case of diamond interchanges, interchanges where there are service roads
> available, or other interchanges where the positioning and layout of the
> ramps is such that they could be used without difficulty to get around
> the structure. If a ground level bypass is available at the structure
> site for the inventory route, record and code the detour length as 000.
>
> If the bridge is one of twin bridges and is not at an interchange, code
> 001 where the other twin bridge can be used as a temporary bypass with a
> reasonable amount of crossover grading. The detour route will be
> established following allowable criteria determined by the governing
> authority. (Some authorities will not allow a designated detour over a
> road or bridge of lesser "quality.") Code 199 for 199 kilometers or
> more.

### usdot\_maintainer\_class

AVAIL added field: classification of the agency listed in the MAINTENANCE_021 field.

> Item 21 - Maintenance Responsibility
>
> The actual name(s) of the agency(s) responsible for the maintenance of the
> structure shall be recorded on the inspection form. The codes below shall be
> used to represent the type of agency that has primary responsibility for
> maintaining the structure.

AVAIL classifies the agency responsible for the maintenance as follows:

|                description                | classification
|-------------------------------------------|----------------
| State Highway Agency                      | State
| County Highway Agency                     | County
| Town or Township Highway Agency           | Municipal
| City or Municipal Highway Agency          | Municipal
| State Park, Forest, or Reservation Agency | State
| Local Park, Forest, or Reservation Agency | Municipal
| Other State Agencies                      | State
| Other Local Agencies                      | Municipal
| Private (other than railroad)             | Other
| Railroad                                  | Railroad
| State Toll Authority                      | State
| Local Toll Authority                      | Municipal
| Veteran Affairs                           | Federal
| Other Federal Agencies (not listed below) | Federal
| Indian Tribal Government                  | Native
| Bureau of Indian Affairs                  | Native
| Bureau of Fish and Wildlife               | Federal
| U.S. Forest Service                       | Federal
| National Park Service                     | Federal
| Tennessee Valley Authority                | Other
| Bureau of Land Management                 | Federal
| Bureau of Reclamation                     | Federal
| Corps of Engineers (Civil)                | Federal
| Corps of Engineers (Military)             | Federal
| Air Force                                 | Federal
| Navy/Marines                              | Federal
| Army                                      | Federal
| NASA                                      | Federal
| Metropolitan Washington Airports Service  | Other
| Unknown                                   | Other

### usdot\_maintainer\_description

USDOT provides the following descriptions for the MAINTENANCE_021 codes:

| code |                description
|------|-------------------------------------------
| 01   | State Highway Agency
| 02   | County Highway Agency
| 03   | Town or Township Highway Agency
| 04   | City or Municipal Highway Agency
| 11   | State Park, Forest, or Reservation Agency
| 12   | Local Park, Forest, or Reservation Agency
| 21   | Other State Agencies
| 25   | Other Local Agencies
| 26   | Private (other than railroad)
| 27   | Railroad
| 31   | State Toll Authority
| 32   | Local Toll Authority
| 56   | Veteran Affairs
| 60   | Other Federal Agencies (not listed below)
| 61   | Indian Tribal Government
| 62   | Bureau of Indian Affairs
| 63   | Bureau of Fish and Wildlife
| 64   | U.S. Forest Service
| 66   | National Park Service
| 67   | Tennessee Valley Authority
| 68   | Bureau of Land Management
| 69   | Bureau of Reclamation
| 70   | Corps of Engineers (Civil)
| 71   | Corps of Engineers (Military)
| 72   | Air Force
| 73   | Navy/Marines
| 74   | Army
| 75   | NASA
| 76   | Metropolitan Washington Airports Service
| 80   | Unknown

### usdot\_owner\_class

AVAIL added field: classification of the agency listed in the OWNER\_022 field.

> Item 22 - Owner 2 digits
>
> The actual name(s) of the owner(s) of the bridge shall be recorded on the
> inspection form. The codes used in Item 21 - Maintenance Responsibility shall
> be used to represent the type of agency that is the primary owner of the
> structure.

Owners are classified using the same table shown for usdot\_maintainer\_class above.

### usdot\_owner\_description

Owners code descriptions are the same as for usdot\_maintainer\_description.

### USDOT Condition Ratings

> Items 58 through 62 - Indicate the Condition Ratings
>
> In order to promote uniformity between bridge inspectors, these guidelines
> will be used to rate and code Items 58, 59, 60, 61, and 62. The use of the
> AASHTO Guide for Commonly Recognized (CoRe) Structural Elements is an
> acceptable alternative to using these rating guidelines for Items 58, 59, 60,
> and 62, provided the FHWA translator computer program is used to convert the
> inspection data to NBI condition ratings for NBI data submittal.
>
> Condition ratings are used to describe the existing, in-place bridge as
> compared to the as-built condition. Evaluation is for the materials related,
> physical condition of the deck, superstructure, and substructure components
> of a bridge. The condition evaluation of channels and channel protection and
> culverts is also included. Condition codes are properly used when they
> provide an overall characterization of the general condition of the entire
> component being rated. Conversely, they are improperly used if they attempt
> to describe localized or nominally occurring instances of deterioration or
> disrepair. Correct assignment of a condition code must, therefore, consider
> both the severity of the deterioration or disrepair and the extent to which
> it is widespread throughout the component being rated.

> The following general condition ratings shall be used as a guide in
evaluating Items 58, 59, and 60:

| code |      short\_description
|------|------------------------
| N    | NOT APPLICABLE
| 9    | EXCELLENT CONDITION
| 8    | VERY GOOD CONDITION
| 7    | GOOD CONDITION
| 6    | SATISFACTORY CONDITION
| 5    | FAIR CONDITION
| 4    | POOR CONDITION
| 3    | SERIOUS CONDITION
| 2    | CRITICAL CONDITION
| 1    | "IMMINENT" FAILURE CONDITION
| 0    | FAILED CONDITION

### usdot\_deck\_condition\_058

> Item 58
>
> This item describes the overall condition rating of the deck.

### usdot\_superstructure\_condition\_059

> Item 59 - Superstructure
>
> This item describes the physical condition of all structural members.

### usdot\_substructure\_condition\_060

> Item 60 - Substructure
>
> This item describes the physical condition of piers, abutments, piles,
> fenders, footings, or other components.

### usdot\_channel\_condition\_061

> Item 61 - Channel and Channel Protection
>
> This item describes the physical conditions associated with the flow of
> water through the bridge such as stream stability and the condition of the
> channel, riprap, slope protection, or stream control devices including
> spur dikes. The inspector should be particularly concerned with visible
> signs of excessive water velocity which may affect undermining of slope
> protection, erosion of banks, and realignment of the stream which may
> result in immediate or potential problems. Accumulation of drift and
> debris on the superstructure and substructure should be noted on the
> inspection form but not included in the condition rating.

| code | description
|------|------------
| N    | Not applicable. Use when bridge is not over a waterway (channel).
| 9    | There are no noticeable or noteworthy deficiencies which affect the condition of the channel.
| 8    | Banks are protected or well vegetated. River control devices such as spur dikes and embankment protection are not required or are in a stable condition.
| 7    | Bank protection is in need of minor repairs. River control devices and embankment protection have a little minor damage. Banks and/or channel have minor amounts of drift.
| 6    | Bank is beginning to slump. River control devices and embankment protection have widespread minor damage. There is minor stream bed movement evident. Debris is restricting the channel slightly.
| 5    | Bank protection is being eroded. River control devices and/or embankment have major damage. Trees and brush restrict the channel.
| 4    | Bank and embankment protection is severely undermined. River control devices have severe damage. Large deposits of debris are in the channel.
| 3    | Bank protection has failed. River control devices have been destroyed. Stream bed aggradation, degradation or lateral movement has changed the channel to now threaten the bridge and/or approach roadway.
| 2    | The channel has changed to the extent the bridge is near a state of collapse.
| 1    | Bridge closed because of channel failure. Corrective action may put back in light service.
| 0    | Bridge closed because of channel failure. Replacement necessary.

### usdot\_culvert\_condition\_062

> Item 62 - Culverts
>
> This item evaluates the alignment, settlement, joints, structural condition,
> scour, and other items associated with culverts. The rating code is intended
> to be an overall condition evaluation of the culvert. Integral wingwalls to
> the first construction or expansion joint shall be included in the
> evaluation. For a detailed discussion regarding the inspection and rating of
> culverts, consult Report No. FHWA-IP-86-2, Culvert Inspection Manual, July
> 1986.
>
> Item 58 - Deck, Item 59 - Superstructure, and Item 60 - Substructure shall be
> coded N for all culverts.
>
> Rate and code the condition in accordance with the previously described
> general condition ratings and the following descriptive codes:

| code | description
|------|-------------
| N    | Not applicable. Use if structure is not a culvert.
| 9    | No deficiencies.
| 8    | No noticeable or noteworthy deficiencies which affect the condition of the culvert. Insignificant scrape marks caused by drift.
| 7    | Shrinkage cracks, light scaling, and insignificant spalling which does not expose reinforcing steel. Insignificant damage caused by drift with no misalignment and not requiring corrective action. Some minor scouring has occurred near curtain walls, wingwalls, or pipes. Metal culverts have a smooth symmetrical curvature with superficial corrosion and no pitting.
| 6    | Deterioration or initial disintegration, minor chloride contamination, cracking with some leaching, or spalls on concrete or masonry walls and slabs. Local minor scouring at curtain walls, wingwalls, or pipes. Metal culverts have a smooth curvature, non-symmetrical shape, significant corrosion or moderate pitting.
| 5    | Moderate to major deterioration or disintegration, extensive cracking and leaching, or spalls on concrete or masonry walls and slabs. Minor settlement or misalignment. Noticeable scouring or erosion at curtain walls, wingwalls, or pipes. Metal culverts have significant distortion and deflection in one section, significant corrosion or deep pitting.
| 4    | Large spalls, heavy scaling, wide cracks, considerable efflorescence, or opened construction joint permitting loss of backfill. Considerable settlement or misalignment. Considerable scouring or erosion at curtain walls, wingwalls or pipes. Metal culverts have significant distortion and deflection throughout, extensive corrosion or deep pitting.
| 3    | Any condition described in Code 4 but which is excessive in scope. Severe movement or differential settlement of the segments, or loss of fill. Holes may exist in walls or slabs. Integral wingwalls nearly severed from culvert. Severe scour or erosion at curtain walls, wingwalls or pipes. Metal culverts have extreme distortion and deflection in one section, extensive corrosion, or deep pitting with scattered perforations.
| 2    | Integral wingwalls collapsed, severe settlement of roadway due to loss of fill. Section of culvert may have failed and can no longer support embankment. Complete undermining at curtain walls and pipes. Corrective action required to maintain traffic. Metal culverts have extreme distortion and deflection throughout with extensive perforations due to corrosion.
| 1    | Bridge closed. Corrective action may put back in light service.
| 0    | Bridge closed. Replacement necessary.


### usdot\_type\_of\_service\_under\_bridge\_code\_042b

> Item 42(b) - Type of Service 
>
> The type of service under the bridge

### usdot\_type\_of\_service\_under\_bridge\_description

Description of usdot\_type\_of\_service\_under\_bridge\_code\_042b,
per the USDOT BTS NBI documentation:

| code | description
|------|-------------------------------------
| 1    | Highway, with or without pedestrian
| 2    | Railroad
| 3    | Pedestrian-bicycle
| 4    | Highway-railroad
| 5    | Waterway
| 6    | Highway-waterway
| 7    | Railroad-waterway
| 8    | Highway-waterway-railroad
| 9    | Relief for waterway
| 0    | Other

### usdot\_features\_desc\_006a

> Item 6 - Features Intersected 25 digits
>
> This item contains a description of the features intersected by the structure
> and a critical facility indicator.

### usdot\_crosses\_water

AVAIL added field: true if usdot\_type\_of\_service\_under\_bridge\_description in (5,6,7,8,9).

### usdot\_longitude

Longitude of the bridge in the NBI dataset.

### usdot\_latitude

Latitude of the bridge in the NBI dataset.

### usdot\_is\_in\_poor\_condition

AVAIL added field: true if any of the following condition ratings are less than 5:

* usdot\_deck\_condition\_058
* usdot\_superstructure\_condition\_059
* usdot\_substructure\_condition\_060
* usdot\_channel\_condition\_061
* usdot\_culvert\_condition\_062

### nysdot\_usdot\_location\_difference\_meters

Distance in meters between the location of the bridge in the NYSDOT and USDOT datasets.

## Resources

### USDOT Bureau of Transportation Statistics National Bridge Inventory

* source: [National Bridge Inventory](https://geodata.bts.gov/datasets/national-bridge-inventory/about)
* documenation: [The Recording and Coding Guide for the Structure Inventory and Appraisal of the Nation's Bridges](https://www.fhwa.dot.gov/bridge/mtguide.pdf)

### NYSDOT Bridges Dataset

* source: [NYSDOT Structures Bridges](https://data.gis.ny.gov/datasets/9e038774ef034c7cae5374f3e23f7a67_0/about?layer=0)
* documentation: [NYSDOT BRIDGE AND LARGE CULVERT INVENTORY MANUAL July 2020](https://www.dot.ny.gov/divisions/engineering/structures/repository/manuals/inventory/NYSDOT\_inventory\_manual\_2020.pdf)


## ISSUES

See the NYS_Bridges_Data_Integrity_Issues report.
