CREATE OR REPLACE VIEW transcom.transcom_historical_events
  AS
    SELECT a.event_id,
      a.event_type,
      a.facility,
      a.creation,
      a.open_time,
      a.close_time,
      a.duration,
      a.state,
      a.from_count,
      a.from_city,
      a.to_city,
      a.description,
      a.from_mile_marker,
      a.to_mile_marker,
      a.latitude,
      a.longitude,
      a.direction,
      a.recovery_time,
      a.recovery_date_time,
      a.event_category,
      a.duration_interval,
      a.point_geom,
      b.congestion_data,
      a._created_timestamp,
      a._modified_timestamp,
      c.event_class,
      d.display_in_incident_dashboard AS nysdot_display_in_incident_dashboard,
      d.general_category AS nysdot_general_category,
      d.sub_category AS nysdot_sub_category,
      d.detailed_category AS nysdot_detailed_category,
      d.waze_category AS nysdot_waze_category,
      d.display_if_lane_closure AS nysdot_display_if_lane_closure,
      d.duration_accurate AS nysdot_duration_accurate
    FROM transcom._transcom_historical_events AS a
      LEFT OUTER JOIN _transcom_admin.transcom_event_congestion_data AS b
        USING (event_id)
      LEFT OUTER JOIN transcom.transcom_event_type_classifications AS c
        ON ( lower(a.event_type) = lower(c.event_type) )
      LEFT OUTER JOIN transcom.nysdot_transcom_event_classifications AS d
        ON ( lower(a.event_type) = lower(d.event_type) )
;
