import _ from "lodash";

export type TranscomEventID = string;
export type TranscomApiRequestTimestamp = string;

export enum TransomEventDirection {
  ALL_DIRECTIONS = "all directions",
  BOTH_DIRECTIONS = "both directions",
  EASTBOUND = "eastbound",
  NEGATIVE_DIRECTION = "negative direction",
  NORTHBOUND = "northbound",
  NOT_DIRECTIONAL = "not directional",
  POSITIVE_DIRECTION = "positive direction",
  SOUTHBOUND = "southbound",
  UNKNOWN = "unknown",
  WESTBOUND = "westbound",
}

export enum TranscomEventStatus {
  CLOSED = "closed",
  NEW = "new",
  UPDATED = "updated",
}

export type TranscomEventType =
  | "abandoned"
  | "accident"
  | "accident investigation"
  | "accident road closed"
  | "accident with injuries"
  | "accident with property damage"
  | "airplane crash"
  | "amber alert"
  | "blackout"
  | "brush fire"
  | "building fire"
  | "bus fire"
  | "capacity related"
  | "cargo spill"
  | "collapsed manhole"
  | "collapsed scaffolding"
  | "collapsed sewer grate"
  | "construction"
  | "crash"
  | "crash investigation"
  | "crash road closed"
  | "crash with fatal"
  | "crash with injuries"
  | "crash with property damage"
  | "debris"
  | "debris spill"
  | "delays"
  | "demonstration"
  | "disabled bus"
  | "disabled tractor trailer"
  | "disabled truck"
  | "disabled vehicle"
  | "downed pole"
  | "downed tree"
  | "downed wires"
  | "drawbridge open"
  | "earlier incident"
  | "earthquake damage"
  | "electrical repairs"
  | "emergency construction"
  | "emergency maintenance"
  | "empty and tandem tractor trailer ban"
  | "empty tractor trailer ban"
  | "ems activity"
  | "equipment malfunction"
  | "falling debris"
  | "falling ice"
  | "fire department activity"
  | "flooding"
  | "fluid spill"
  | "fog"
  | "forest fire"
  | "fuel spill"
  | "full commercial vehicle ban"
  | "funeral procession"
  | "gas main break"
  | "grade crossing accident"
  | "gridlock alert day"
  | "hazmat spill"
  | "heavy snow"
  | "heavy traffic"
  | "high winds"
  | "hov rules"
  | "hurricane"
  | "icicle removal"
  | "icing"
  | "jack-knifed tractor trailer"
  | "job action"
  | "landslide"
  | "long term road construction"
  | "malfunctioning traffic light"
  | "minor delays"
  | "misplaced bus"
  | "misplaced commercial vehicle"
  | "misplaced tractor trailer"
  | "missing manhole cover"
  | "missing sewer grate"
  | "motorcycle rally"
  | "nearby building collapse"
  | "new traffic pattern"
  | "no diesel available"
  | "no food available"
  | "no fuel available"
  | "non-vehicle fire/explosion"
  | "no water available"
  | "operational activity"
  | "other"
  | "outside agency assist"
  | "overhead sign repairs"
  | "overheight tractor trailer"
  | "overturned dump truck"
  | "overturned tractor trailer"
  | "overturned truck"
  | "overturned vehicle"
  | "ozone alert"
  | "parking related"
  | "pedestrian"
  | "pedestrian accident"
  | "plowing and salting"
  | "pockets of volume"
  | "police department activity"
  | "pothole repairs"
  | "power failure"
  | "power problems"
  | "report of an incident"
  | "road collapse"
  | "road sweeping"
  | "road treatment in progress"
  | "roadway non-hazmat spill"
  | "roadwork"
  | "rough road"
  | "roving repairs"
  | "rubbernecking"
  | "rubbernecking delays"
  | "security check point"
  | "security related"
  | "separated tractor trailer"
  | "sewer collapse"
  | "sewer main break"
  | "shifted plates"
  | "signal problem"
  | "silver alert"
  | "sinkhole"
  | "slow moving operation"
  | "snow on roadway"
  | "snow removal"
  | "special event"
  | "speed advisory"
  | "speed restriction"
  | "spinout"
  | "split tractor trailer"
  | "steam leak"
  | "stop and go traffic"
  | "storm damage"
  | "street light knockdown"
  | "strorm damage"
  | "stuck gates"
  | "sun glare"
  | "test message"
  | "tornado"
  | "tractor trailer ban"
  | "tractor trailer fire"
  | "traffic congestion"
  | "traffic signal down"
  | "transformer fire"
  | "transit related incident"
  | "truck fire"
  | "truck restrictions"
  | "unanticipated construction delay"
  | "unplanned demonstration"
  | "utility pole down"
  | "utility work"
  | "vehicle fire"
  | "vehicle off the roadway"
  | "vehicle spun out"
  | "vip visit"
  | "watermain break"
  | "weather related"
  | "wet pavement"
  | "wide load";
// Straight from the TRANSCOM API
export type RawTranscomEvent = {
  facility: string;
  eventType: string | null;
  summaryDescription: string | null;
  state: string;
  county: string | null;
  city: string | null;
  lastUpdate: string;
  manualCloseDate: string | null;
  eventDuration: string | null;
  id: string;
  startDateTime: string;
  linkCount: null;
  ToCity: string | null;
  secondaryMarker: string | null;
  pointLAT: number | null;
  pointLON: number | null;
  PrimaryMarker: string | null;
  FromCity: string | null;
  eventTypeDescId: number;
  eventCategory: string;
  reportingOrgId: number;
  direction: string | null;
  eventstatus: string | null;
  year: string | null;
  dataSource: string | null;
  dataSourceValue: null;
  tmclist: string | null;
  recoverytime: null;
  RecoveryTimeInFormate: null;
  recoverydatetime: null;
  isHighway: number;
};

// "Proto" because prior to value normalization.
export type ProtoTranscomEvent = {
  event_id: TranscomEventID;
  facility: RawTranscomEvent["facility"];
  event_type: RawTranscomEvent["eventType"];
  summary_description: RawTranscomEvent["summaryDescription"];
  state: RawTranscomEvent["state"];
  county: RawTranscomEvent["county"];
  city: RawTranscomEvent["city"];
  last_update: RawTranscomEvent["lastUpdate"];
  manual_close_date: RawTranscomEvent["manualCloseDate"];
  event_duration: RawTranscomEvent["eventDuration"];
  start_date_time: RawTranscomEvent["startDateTime"];
  link_count: RawTranscomEvent["linkCount"];
  to_city: RawTranscomEvent["ToCity"];
  secondary_marker: RawTranscomEvent["secondaryMarker"];
  point_lat: RawTranscomEvent["pointLAT"];
  point_lon: RawTranscomEvent["pointLON"];
  primary_marker: RawTranscomEvent["PrimaryMarker"];
  from_city: RawTranscomEvent["FromCity"];
  event_type_desc_id: RawTranscomEvent["eventTypeDescId"];
  event_category: RawTranscomEvent["eventCategory"];
  reporting_org_id: RawTranscomEvent["reportingOrgId"];
  direction: RawTranscomEvent["direction"];
  eventstatus: RawTranscomEvent["eventstatus"];
  year: RawTranscomEvent["year"];
  data_source: RawTranscomEvent["dataSource"];
  data_source_value: RawTranscomEvent["dataSourceValue"];
  tmclist: RawTranscomEvent["tmclist"];
  recoverytime: RawTranscomEvent["recoverytime"];
  recovery_time_in_formate: RawTranscomEvent["RecoveryTimeInFormate"];
  recoverydatetime: RawTranscomEvent["recoverydatetime"];
  is_highway: RawTranscomEvent["isHighway"];
};

export type TranscomEvent = {
  event_id: TranscomEventID | null;
  facility: string | null;
  event_type: TranscomEventType | null;
  summary_description: string | null;
  state: string | null;
  county: string | null;
  city: string | null;
  last_update: Date | null;
  manual_close_date: Date | null;
  event_duration: string | null;
  start_date_time: Date | null;
  link_count: string | null;
  to_city: string | null;
  secondary_marker: number | null;
  point_lat: number | null;
  point_lon: number | null;
  primary_marker: number | null;
  from_city: string | null;
  event_type_desc_id: number | null;
  event_category: string | null;
  reporting_org_id: number | null;
  direction: string | null;
  eventstatus: string | null;
  year: number | null;
  data_source: boolean | null;
  data_source_value: string | null;
  tmclist: string | null;
  recoverytime: string | null;
  recovery_time_in_formate: string | null;
  recoverydatetime: string | null;
  is_highway: boolean | null;
};

export type RawTranscomEventExpanded = {
  ID: TranscomEventID;
  "Event Class": string;
  "Reporting Organization": string;
  "Start DateTime": string;
  "End DateTime": null;
  "Last Updatedate": string;
  "Close Date": string | null;
  Estimated_Duration_Mins: number;
  eventDuration: string | null;
  Facility: string;
  "Event Type": string;
  "Lanes Total Count": number | null;
  "Lanes Affected Count": number | null;
  "Lanes Detail": string | null;
  "Lanes Status": string | null;
  Description: string;
  Direction: string | null;
  County: string | null;
  City: string | null;
  "City Article": string | null;
  "Primary City": null;
  "Secondary City": string | null;
  PointLAT: number;
  PointLONG: number;
  "Location Article": string | null;
  "Primary Marker": number | null;
  "Secondary Marker": number | null;
  "Primary location": string | null;
  "Secondary location": string | null;
  State: string;
  "Region Closed": boolean;
  "Point Datum": string;
  "Marker Units": null;
  "Marker Article": string | null;
  SummaryDescription: string;
  Eventstatus: string;
  isHighway: number;
  IconFile: string;
  StartIncidentOccured: string | null;
  StartedAtDateTimeComment: null;
  IncidentReported: string | null;
  IncidentReportedComment: null;
  IncidentVerified: string | null;
  IncidentVerifiedComment: null;
  ResponseIdentifiedAndDispatched: string | null;
  ResponseIdentifiedAndDispatchedComment: null;
  ResponseArrivesonScene: string | null;
  ResponseArrivesonSceneComment: string | null;
  EndAllLanesOpenToTraffic: string | null;
  EndedAtDateTimeComment: null;
  ResponseDepartsScene: string | null;
  ResponseDepartsSceneComment: null;
  TimeToReturnToNormalFlow: string | null;
  TimeToReturnToNormalFlowComment: null;
  NoOfVehicleInvolved: string | null;
  SecondaryEvent: boolean | null;
  SecondaryEventTypes: null;
  SecondaryInvolvements: null;
  WithinWorkZone: boolean | null;
  TruckCommercialVehicleInvolved: boolean | null;
  ShoulderAvailable: boolean | null;
  InjuryInvolved: boolean | null;
  FatalityInvolved: boolean | null;
  MaintanceCrewInvolved: boolean | null;
  RoadwayClearance: string;
  IncidentClearance: string;
  TimeToReturnToNormalFlowDuration: string;
  Duration: string;
  AssociatedImpactIds: string | null;
  SecondaryEventIds: string | null;
  IsTransit: boolean;
  IsShoulderLane: boolean | null;
  IsTollLane: boolean | null;
  LanesAffectedDetail: string | null;
  ToFacility: string;
  ToState: string;
  ToDirection: string | null;
  fatalityInvolved_associatedEventID: string | null;
  withInWorkZone_associatedEventID: string | null;
  ToLat: number;
  ToLon: number;
  PrimaryDirection: string | null;
  SecondaryDirection: string | null;
  IsBothDirection: boolean;
  "Secondary Lanes Affected Count": number | null;
  "Secondary Lanes Detail": string | null;
  "Secondary Lanes Status": string | null;
  "Secondary Lanes Total Count": number | null;
  SecondaryLanesAffectedDetail: string | null;
  EventLocationLatitude: number | null;
  EventLocationLongitude: number | null;
  tripcnt: number;
  Tmclist: string | null;
  Recoverytime: number | null;
  Year: number;
  Datasource: string;
  Datasourcevalue: string;
  DayofWeek: number | null;
  tmc_geometry: string | null;
};

export type ProtoTranscomEventExpanded = {
  event_id: RawTranscomEventExpanded["ID"];
  event_class: RawTranscomEventExpanded["Event Class"];
  reporting_organization: RawTranscomEventExpanded["Reporting Organization"];
  start_date_time: RawTranscomEventExpanded["Start DateTime"];
  end_date_time: RawTranscomEventExpanded["End DateTime"];
  last_updatedate: RawTranscomEventExpanded["Last Updatedate"];
  close_date: RawTranscomEventExpanded["Close Date"];
  estimated_duration_mins: RawTranscomEventExpanded["Estimated_Duration_Mins"];
  event_duration: RawTranscomEventExpanded["eventDuration"];
  facility: RawTranscomEventExpanded["Facility"];
  event_type: RawTranscomEventExpanded["Event Type"];
  lanes_total_count: RawTranscomEventExpanded["Lanes Total Count"];
  lanes_affected_count: RawTranscomEventExpanded["Lanes Affected Count"];
  lanes_detail: RawTranscomEventExpanded["Lanes Detail"];
  lanes_status: RawTranscomEventExpanded["Lanes Status"];
  description: RawTranscomEventExpanded["Description"];
  direction: RawTranscomEventExpanded["Direction"];
  county: RawTranscomEventExpanded["County"];
  city: RawTranscomEventExpanded["City"];
  city_article: RawTranscomEventExpanded["City Article"];
  primary_city: RawTranscomEventExpanded["Primary City"];
  secondary_city: RawTranscomEventExpanded["Secondary City"];
  point_lat: RawTranscomEventExpanded["PointLAT"];
  point_long: RawTranscomEventExpanded["PointLONG"];
  location_article: RawTranscomEventExpanded["Location Article"];
  primary_marker: RawTranscomEventExpanded["Primary Marker"];
  secondary_marker: RawTranscomEventExpanded["Secondary Marker"];
  primary_location: RawTranscomEventExpanded["Primary location"];
  secondary_location: RawTranscomEventExpanded["Secondary location"];
  state: RawTranscomEventExpanded["State"];
  region_closed: RawTranscomEventExpanded["Region Closed"];
  point_datum: RawTranscomEventExpanded["Point Datum"];
  marker_units: RawTranscomEventExpanded["Marker Units"];
  marker_article: RawTranscomEventExpanded["Marker Article"];
  summary_description: RawTranscomEventExpanded["SummaryDescription"];
  eventstatus: RawTranscomEventExpanded["Eventstatus"];
  is_highway: RawTranscomEventExpanded["isHighway"];
  icon_file: RawTranscomEventExpanded["IconFile"];
  start_incident_occured: RawTranscomEventExpanded["StartIncidentOccured"];
  started_at_date_time_comment: RawTranscomEventExpanded["StartedAtDateTimeComment"];
  incident_reported: RawTranscomEventExpanded["IncidentReported"];
  incident_reported_comment: RawTranscomEventExpanded["IncidentReportedComment"];
  incident_verified: RawTranscomEventExpanded["IncidentVerified"];
  incident_verified_comment: RawTranscomEventExpanded["IncidentVerifiedComment"];
  response_identified_and_dispatched: RawTranscomEventExpanded["ResponseIdentifiedAndDispatched"];
  response_identified_and_dispatched_comment: RawTranscomEventExpanded["ResponseIdentifiedAndDispatchedComment"];
  response_arriveson_scene: RawTranscomEventExpanded["ResponseArrivesonScene"];
  response_arriveson_scene_comment: RawTranscomEventExpanded["ResponseArrivesonSceneComment"];
  end_all_lanes_open_to_traffic: RawTranscomEventExpanded["EndAllLanesOpenToTraffic"];
  ended_at_date_time_comment: RawTranscomEventExpanded["EndedAtDateTimeComment"];
  response_departs_scene: RawTranscomEventExpanded["ResponseDepartsScene"];
  response_departs_scene_comment: RawTranscomEventExpanded["ResponseDepartsSceneComment"];
  time_to_return_to_normal_flow: RawTranscomEventExpanded["TimeToReturnToNormalFlow"];
  time_to_return_to_normal_flow_comment: RawTranscomEventExpanded["TimeToReturnToNormalFlowComment"];
  no_of_vehicle_involved: RawTranscomEventExpanded["NoOfVehicleInvolved"];
  secondary_event: RawTranscomEventExpanded["SecondaryEvent"];
  secondary_event_types: RawTranscomEventExpanded["SecondaryEventTypes"];
  secondary_involvements: RawTranscomEventExpanded["SecondaryInvolvements"];
  within_work_zone: RawTranscomEventExpanded["WithinWorkZone"];
  truck_commercial_vehicle_involved: RawTranscomEventExpanded["TruckCommercialVehicleInvolved"];
  shoulder_available: RawTranscomEventExpanded["ShoulderAvailable"];
  injury_involved: RawTranscomEventExpanded["InjuryInvolved"];
  fatality_involved: RawTranscomEventExpanded["FatalityInvolved"];
  maintance_crew_involved: RawTranscomEventExpanded["MaintanceCrewInvolved"];
  roadway_clearance: RawTranscomEventExpanded["RoadwayClearance"];
  incident_clearance: RawTranscomEventExpanded["IncidentClearance"];
  time_to_return_to_normal_flow_duration: RawTranscomEventExpanded["TimeToReturnToNormalFlowDuration"];
  duration: RawTranscomEventExpanded["Duration"];
  associated_impact_ids: RawTranscomEventExpanded["AssociatedImpactIds"];
  secondary_event_ids: RawTranscomEventExpanded["SecondaryEventIds"];
  is_transit: RawTranscomEventExpanded["IsTransit"];
  is_shoulder_lane: RawTranscomEventExpanded["IsShoulderLane"];
  is_toll_lane: RawTranscomEventExpanded["IsTollLane"];
  lanes_affected_detail: RawTranscomEventExpanded["LanesAffectedDetail"];
  to_facility: RawTranscomEventExpanded["ToFacility"];
  to_state: RawTranscomEventExpanded["ToState"];
  to_direction: RawTranscomEventExpanded["ToDirection"];
  fatality_involved_associated_event_id: RawTranscomEventExpanded["fatalityInvolved_associatedEventID"];
  with_in_work_zone_associated_event_id: RawTranscomEventExpanded["withInWorkZone_associatedEventID"];
  to_lat: RawTranscomEventExpanded["ToLat"];
  to_lon: RawTranscomEventExpanded["ToLon"];
  primary_direction: RawTranscomEventExpanded["PrimaryDirection"];
  secondary_direction: RawTranscomEventExpanded["SecondaryDirection"];
  is_both_direction: RawTranscomEventExpanded["IsBothDirection"];
  secondary_lanes_affected_count: RawTranscomEventExpanded["Secondary Lanes Affected Count"];
  secondary_lanes_detail: RawTranscomEventExpanded["Secondary Lanes Detail"];
  secondary_lanes_status: RawTranscomEventExpanded["Secondary Lanes Status"];
  secondary_lanes_total_count: RawTranscomEventExpanded["Secondary Lanes Total Count"];
  secondary_lanes_affected_detail: RawTranscomEventExpanded["SecondaryLanesAffectedDetail"];
  event_location_latitude: RawTranscomEventExpanded["EventLocationLatitude"];
  event_location_longitude: RawTranscomEventExpanded["EventLocationLongitude"];
  tripcnt: RawTranscomEventExpanded["tripcnt"];
  tmclist: RawTranscomEventExpanded["Tmclist"];
  recoverytime: RawTranscomEventExpanded["Recoverytime"];
  year: RawTranscomEventExpanded["Year"];
  datasource: RawTranscomEventExpanded["Datasource"];
  datasourcevalue: RawTranscomEventExpanded["Datasourcevalue"];
  dayof_week: RawTranscomEventExpanded["DayofWeek"];
  tmc_geometry: RawTranscomEventExpanded["tmc_geometry"];
};

export type TranscomEventExpanded = {
  event_id: TranscomEventID;
  event_class: string | null;
  reporting_organization: string | null;
  start_date_time: Date | null;
  end_date_time: string | null;
  last_updatedate: Date | null;
  close_date: Date | null;
  estimated_duration_mins: number | null;
  event_duration: string | null;
  facility: string | null;
  event_type: TranscomEventType | null;
  lanes_total_count: number | null;
  lanes_affected_count: number | null;
  lanes_detail: string | null;
  lanes_status: string | null;
  description: string | null;
  direction: string | null;
  county: string | null;
  city: string | null;
  city_article: string | null;
  primary_city: string | null;
  secondary_city: string | null;
  point_lat: number | null;
  point_long: number | null;
  location_article: string | null;
  primary_marker: number | null;
  secondary_marker: number | null;
  primary_location: string | null;
  secondary_location: string | null;
  state: string | null;
  region_closed: boolean | null;
  point_datum: string | null;
  marker_units: string | null;
  marker_article: string | null;
  summary_description: string | null;
  eventstatus: string | null;
  is_highway: boolean | null;
  icon_file: string | null;
  start_incident_occured: Date | null;
  started_at_date_time_comment: string | null;
  incident_reported: Date | null;
  incident_reported_comment: string | null;
  incident_verified: Date | null;
  incident_verified_comment: string | null;
  response_identified_and_dispatched: Date | null;
  response_identified_and_dispatched_comment: string | null;
  response_arriveson_scene: Date | null;
  response_arriveson_scene_comment: string | null;
  end_all_lanes_open_to_traffic: Date | null;
  ended_at_date_time_comment: string | null;
  response_departs_scene: Date | null;
  response_departs_scene_comment: string | null;
  time_to_return_to_normal_flow: Date | null;
  time_to_return_to_normal_flow_comment: string | null;
  no_of_vehicle_involved: string | null;
  secondary_event: boolean | null;
  secondary_event_types: string | null;
  secondary_involvements: string | null;
  within_work_zone: boolean | null;
  truck_commercial_vehicle_involved: boolean | null;
  shoulder_available: boolean | null;
  injury_involved: boolean | null;
  fatality_involved: boolean | null;
  maintance_crew_involved: boolean | null;
  roadway_clearance: string | null;
  incident_clearance: string | null;
  time_to_return_to_normal_flow_duration: string | null;
  duration: string | null;
  associated_impact_ids: string | null;
  secondary_event_ids: string | null;
  is_transit: boolean | null;
  is_shoulder_lane: boolean | null;
  is_toll_lane: boolean | null;
  lanes_affected_detail: string | null;
  to_facility: string | null;
  to_state: string | null;
  to_direction: string | null;
  fatality_involved_associated_event_id: boolean | null;
  with_in_work_zone_associated_event_id: string | null;
  to_lat: number | null;
  to_lon: number | null;
  primary_direction: string | null;
  secondary_direction: string | null;
  is_both_direction: boolean | null;
  secondary_lanes_affected_count: number | null;
  secondary_lanes_detail: string | null;
  secondary_lanes_status: string | null;
  secondary_lanes_total_count: number | null;
  secondary_lanes_affected_detail: string | null;
  event_location_latitude: number | null;
  event_location_longitude: number | null;
  tripcnt: boolean | null;
  tmclist: string | null;
  recoverytime: number | null;
  year: number | null;
  datasource: boolean | null;
  datasourcevalue: string | null;
  dayof_week: number | null;
  tmc_geometry: string | null;
};

export const apiResponsePropsToDbCols = {
  ID: "event_id",
  "Event Class": "event_class",
  "Reporting Organization": "reporting_organization",
  "Start DateTime": "start_date_time",
  "End DateTime": "end_date_time",
  "Last Updatedate": "last_updatedate",
  "Close Date": "close_date",
  Estimated_Duration_Mins: "estimated_duration_mins",
  eventDuration: "event_duration",
  Facility: "facility",
  "Event Type": "event_type",
  "Lanes Total Count": "lanes_total_count",
  "Lanes Affected Count": "lanes_affected_count",
  "Lanes Detail": "lanes_detail",
  "Lanes Status": "lanes_status",
  Description: "description",
  Direction: "direction",
  County: "county",
  City: "city",
  "City Article": "city_article",
  "Primary City": "primary_city",
  "Secondary City": "secondary_city",
  PointLAT: "point_lat",
  PointLONG: "point_long",
  "Location Article": "location_article",
  "Primary Marker": "primary_marker",
  "Secondary Marker": "secondary_marker",
  "Primary location": "primary_location",
  "Secondary location": "secondary_location",
  State: "state",
  "Region Closed": "region_closed",
  "Point Datum": "point_datum",
  "Marker Units": "marker_units",
  "Marker Article": "marker_article",
  SummaryDescription: "summary_description",
  Eventstatus: "eventstatus",
  isHighway: "is_highway",
  IconFile: "icon_file",
  StartIncidentOccured: "start_incident_occured",
  StartedAtDateTimeComment: "started_at_date_time_comment",
  IncidentReported: "incident_reported",
  IncidentReportedComment: "incident_reported_comment",
  IncidentVerified: "incident_verified",
  IncidentVerifiedComment: "incident_verified_comment",
  ResponseIdentifiedAndDispatched: "response_identified_and_dispatched",
  ResponseIdentifiedAndDispatchedComment:
    "response_identified_and_dispatched_comment",
  ResponseArrivesonScene: "response_arrives_on_scene",
  ResponseArrivesonSceneComment: "response_arrives_on_scene_comment",
  EndAllLanesOpenToTraffic: "end_all_lanes_open_to_traffic",
  EndedAtDateTimeComment: "ended_at_date_time_comment",
  ResponseDepartsScene: "response_departs_scene",
  ResponseDepartsSceneComment: "response_departs_scene_comment",
  TimeToReturnToNormalFlow: "time_to_return_to_normal_flow",
  TimeToReturnToNormalFlowComment: "time_to_return_to_normal_flow_comment",
  NoOfVehicleInvolved: "no_of_vehicle_involved",
  SecondaryEvent: "secondary_event",
  SecondaryEventTypes: "secondary_event_types",
  SecondaryInvolvements: "secondary_involvements",
  WithinWorkZone: "within_work_zone",
  TruckCommercialVehicleInvolved: "truck_commercial_vehicle_involved",
  ShoulderAvailable: "shoulder_available",
  InjuryInvolved: "injury_involved",
  FatalityInvolved: "fatality_involved",
  MaintanceCrewInvolved: "maintance_crew_involved",
  RoadwayClearance: "roadway_clearance",
  IncidentClearance: "incident_clearance",
  TimeToReturnToNormalFlowDuration: "time_to_return_to_normal_flow_duration",
  Duration: "duration",
  AssociatedImpactIds: "associated_impact_ids",
  SecondaryEventIds: "secondary_event_ids",
  IsTransit: "is_transit",
  IsShoulderLane: "is_shoulder_lane",
  IsTollLane: "is_toll_lane",
  LanesAffectedDetail: "lanes_affected_detail",
  ToFacility: "to_facility",
  ToState: "to_state",
  ToDirection: "to_direction",
  fatalityInvolved_associatedEventID: "fatality_involved_associated_event_id",
  withInWorkZone_associatedEventID: "with_in_work_zone_associated_event_id",
  ToLat: "to_lat",
  ToLon: "to_lon",
  PrimaryDirection: "primary_direction",
  SecondaryDirection: "secondary_direction",
  IsBothDirection: "is_both_direction",
  "Secondary Lanes Affected Count": "secondary_lanes_affected_count",
  "Secondary Lanes Detail": "secondary_lanes_detail",
  "Secondary Lanes Status": "secondary_lanes_status",
  "Secondary Lanes Total Count": "secondary_lanes_total_count",
  SecondaryLanesAffectedDetail: "secondary_lanes_affected_detail",
  EventLocationLatitude: "event_location_latitude",
  EventLocationLongitude: "event_location_longitude",
  tripcnt: "tripcnt",
  Tmclist: "tmclist",
  Recoverytime: "recoverytime",
  Year: "year",
  Datasource: "datasource",
  Datasourcevalue: "datasourcevalue",
  DayofWeek: "day_of_week",
  tmc_geometry: "tmc_geometry",
};

export const dbColsToApiResponseProps = _.invert(apiResponsePropsToDbCols);

// NOTE: dbCols array ensures same order of columns in CSV and COPY FROM CSV statement.
export const dbCols = Object.keys(dbColsToApiResponseProps);
