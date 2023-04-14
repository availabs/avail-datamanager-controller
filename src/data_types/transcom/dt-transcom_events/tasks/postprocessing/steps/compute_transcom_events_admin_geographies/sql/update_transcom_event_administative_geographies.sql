CREATE TEMPORARY TABLE tmp_events_geoms
  ON COMMIT DROP
  AS
    SELECT
        a.event_id,
        a.point_geom AS wkb_geometry,
        a._modified_timestamp AS _latest_modified_timestamp
      FROM _transcom_admin.transcom_events_expanded_view AS a
        LEFT OUTER JOIN _transcom_admin.transcom_event_administative_geographies AS b
          USING (event_id)
      WHERE (
        -- New event
        ( b.event_id IS NULL )
        OR
        -- Updated event
        ( a._modified_timestamp > b._latest_modified_timestamp )
      )
;

-- point_geom will be in the VIEW
CREATE INDEX IF NOT EXISTS tmp_events_geoms_gix
  ON tmp_events_geoms
    USING GIST (wkb_geometry)
;

CLUSTER tmp_events_geoms
  USING tmp_events_geoms_gix
;

ANALYZE tmp_events_geoms ;

DELETE FROM _transcom_admin.transcom_event_administative_geographies
  WHERE (
    event_id IN (
      SELECT
          event_id
        FROM tmp_events_geoms
    )
  )
;

INSERT INTO _transcom_admin.transcom_event_administative_geographies (
  event_id,
  state_name,
  state_code,

  region_name,
  region_code,

  county_name,
  county_code,

  _latest_modified_timestamp
)
  SELECT
        a.event_id,
        b.state AS state_name,
        b.state_code,

        d.name AS region_name,
        c.region AS region_code,
        
        b.county AS county_name,
        a.county_code,

        a._latest_modified_timestamp
    FROM (
      SELECT DISTINCT ON (event_id)
          event_id,
          geoid AS county_code,
          _latest_modified_timestamp
        FROM public.tl_2017_us_county AS x
          INNER JOIN tmp_events_geoms AS y
            ON (
              public.ST_Intersects(
                x.geom,
                y.wkb_geometry
              )
            )
        ORDER BY event_id, county_code
    ) AS a
      INNER JOIN public.fips_codes AS b
        ON (a.county_code = (b.state_code || b.county_code))
      LEFT OUTER JOIN ny.nysdot_regions AS c
        ON (a.county_code = c.fips_code)
      LEFT OUTER JOIN ny.nysdot_region_names AS d
        ON (c.region = d.region)
;

UPDATE _transcom_admin.transcom_event_administative_geographies AS a
  SET 
      mpo_name = b.mpo_name,
      mpo_code = b.mpo_code
    FROM (
      SELECT DISTINCT ON (event_id)
          y.event_id,
          x.mpo_name,
          x.mpo_id AS mpo_code
        FROM public.mpo_boundaries_view AS x
          INNER JOIN tmp_events_geoms AS y
          ON (
            public.ST_Intersects(
              x.wkb_geometry,
              y.wkb_geometry
            )
          )
        ORDER BY event_id, mpo_code
    ) AS b
    WHERE ( a.event_id = b.event_id )
;

CREATE TEMPORARY TABLE tmp_ua_boundaries
  ON COMMIT DROP
  AS
    WITH cte_tmp_events_geom_envelope AS (
      SELECT
          public.ST_SetSRID(
            public.ST_Extent(wkb_geometry),
            4326
          ) AS tmp_events_extent
        FROM tmp_events_geoms
    )
    SELECT
        a.name10 AS ua_name,
        a.geoid10 AS ua_code,
        a.wkb_geometry
      FROM public.urban_area_boundaries AS a
        INNER JOIN cte_tmp_events_geom_envelope
          ON (
            public.ST_Intersects(
              tmp_events_extent,
              a.wkb_geometry
            )
          )
;

CREATE INDEX IF NOT EXISTS tmp_ua_boundaries_gix
  ON tmp_ua_boundaries
    USING GIST (wkb_geometry)
;

CLUSTER tmp_ua_boundaries USING tmp_ua_boundaries_gix ;
ANALYZE tmp_ua_boundaries ;

UPDATE _transcom_admin.transcom_event_administative_geographies AS a
  SET 
      ua_name = b.ua_name,
      ua_code = b.ua_code
    FROM (
      SELECT DISTINCT ON (event_id)
          y.event_id,
          x.ua_name,
          x.ua_code
        FROM tmp_ua_boundaries AS x
          INNER JOIN tmp_events_geoms AS y
          ON (
            public.ST_Intersects(
              x.wkb_geometry,
              y.wkb_geometry
            )
          )
        ORDER BY event_id, ua_code
    ) AS b
    WHERE ( a.event_id = b.event_id )
;

DROP TABLE tmp_events_geoms ;

CLUSTER _transcom_admin.transcom_event_administative_geographies;
ANALYZE _transcom_admin.transcom_event_administative_geographies;
