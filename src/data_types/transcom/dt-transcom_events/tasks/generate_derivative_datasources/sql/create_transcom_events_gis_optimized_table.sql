CREATE TABLE :"staging_schema".transcom_events_gis_optimized
  WITH ( fillfactor = 100 )
  AS
    SELECT
        a.event_id,
        b.year,
        LOWER(a.direction) AS direction,

        public.ST_Transform(
          public.ST_SetSRID(
            public.ST_MakePoint(
              point_long,
              point_lat
            ),
            4269 -- NAD83 -- EPSG:4269
          ),
          4326  -- EPSG:4326
        ) AS point_geom

      FROM :"staging_schema".transcom_events_expanded AS a
        LEFT JOIN LATERAL generate_series(
          EXTRACT(
            YEAR FROM start_date_time
          )::INTEGER,
          EXTRACT(
            YEAR FROM COALESCE(end_all_lanes_open_to_traffic, close_date)
          )::INTEGER
        ) AS b(year) ON TRUE
      WHERE (
        ( a.state = 'NY')
        AND
        ( a.point_long IS NOT NULL )
        AND
        ( a.point_lat IS NOT NULL )
        AND
        -- A conflation map must exist for the year.
        ( b.year BETWEEN :min_year AND :max_year )
      )
;

CREATE INDEX transcom_events_gis_optimized_gix
  ON :"staging_schema".transcom_events_gis_optimized
    USING GIST (point_geom)
;

CLUSTER :"staging_schema".transcom_events_gis_optimized
  USING transcom_events_gis_optimized_gix ;

ANALYZE :"staging_schema".transcom_events_gis_optimized ;

CREATE VIEW :"staging_schema".event_geoms
  AS
    SELECT DISTINCT ON (event_id)
        event_id,
        point_geom AS wkb_geometry
      FROM :"staging_schema".transcom_events_gis_optimized
;
