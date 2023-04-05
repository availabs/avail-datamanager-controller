import _ from "lodash";

export const url =
  "https://eventsearch.xcmdata.org/HistoricalEventSearch/xcmEvent/getEventGridData";

export const apiResponsePropsToDbCols = {
  facility: "facility",
  eventType: "event_type",
  summaryDescription: "summary_description",
  state: "state",
  county: "county",
  city: "city",
  lastUpdate: "last_update",
  manualCloseDate: "manual_close_date",
  eventDuration: "event_duration",
  id: "event_id",
  startDateTime: "start_date_time",
  linkCount: "link_count",
  ToCity: "to_city",
  secondaryMarker: "secondary_marker",
  pointLAT: "point_lat",
  pointLON: "point_lon",
  PrimaryMarker: "primary_marker",
  FromCity: "from_city",
  eventTypeDescId: "event_type_desc_id",
  eventCategory: "event_category",
  reportingOrgId: "reporting_org_id",
  direction: "direction",
  eventstatus: "eventstatus",
  year: "year",
  dataSource: "data_source",
  dataSourceValue: "data_source_value",
  tmclist: "tmclist",
  recoverytime: "recoverytime",
  RecoveryTimeInFormate: "recovery_time_in_formate",
  recoverydatetime: "recoverydatetime",
  isHighway: "is_highway",
};

export const dbColsToApiResponseProps = _.invert(apiResponsePropsToDbCols);

export const dbCols = Object.keys(dbColsToApiResponseProps);
