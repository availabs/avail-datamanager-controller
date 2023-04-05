import {
  TranscomEventID,
  TranscomEventType,
  TranscomEventStatus,
  TransomEventDirection,
} from "../domain/index.d";

export * from "../domain/index.d";

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
