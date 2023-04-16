CREATE TABLE :"staging_schema".transcom_event_administative_geographies (
  LIKE _transcom_admin.transcom_event_administative_geographies
  INCLUDING ALL EXCLUDING CONSTRAINTS
) ;

INSERT INTO :"staging_schema".transcom_event_administative_geographies (
  event_id,
  state_name,
  state_code,

  region_name,
  region_code,

  county_name,
  county_code
)
  SELECT
      a.event_id,
      b.state AS state_name,
      b.state_code,

      d.name AS region_name,
      c.region AS region_code,
      
      b.county AS county_name,
      a.county_code
    FROM (
      SELECT DISTINCT ON (event_id)
          y.event_id,
          x.geoid AS county_code
        FROM public.tl_2017_us_county AS x
          INNER JOIN :"staging_schema".event_geoms AS y
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

UPDATE :"staging_schema".transcom_event_administative_geographies AS a
  SET 
      mpo_name = b.mpo_name,
      mpo_code = b.mpo_code
    FROM (
      SELECT DISTINCT ON (event_id)
          y.event_id,
          x.mpo_name,
          x.mpo_id AS mpo_code
        FROM public.mpo_boundaries_view AS x
          INNER JOIN :"staging_schema".event_geoms AS y
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

CREATE TABLE :"staging_schema".ua_boundaries
  AS
    SELECT
        a.name10 AS ua_name,
        a.geoid10 AS ua_code,
        a.wkb_geometry
      FROM public.urban_area_boundaries AS a
        INNER JOIN (
            SELECT
                public.ST_SetSRID(
                  public.ST_Extent(wkb_geometry),
                  4326
                ) AS tmp_events_extent
              FROM :"staging_schema".event_geoms
          ) as b ON (
            public.ST_Intersects(
              b.tmp_events_extent,
              a.wkb_geometry
            )
          )
;

CREATE INDEX IF NOT EXISTS ua_boundaries_gix
  ON :"staging_schema".ua_boundaries
    USING GIST (wkb_geometry)
;

CLUSTER :"staging_schema".ua_boundaries USING ua_boundaries_gix ;
ANALYZE :"staging_schema".ua_boundaries ;

UPDATE :"staging_schema".transcom_event_administative_geographies AS a
  SET 
      ua_name = b.ua_name,
      ua_code = b.ua_code
    FROM (
      SELECT DISTINCT ON (event_id)
          y.event_id,
          x.ua_name,
          x.ua_code
        FROM :"staging_schema".ua_boundaries AS x
          INNER JOIN :"staging_schema".event_geoms AS y
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

CLUSTER :"staging_schema".transcom_event_administative_geographies
  USING transcom_event_administative_geographies_pkey ;

ANALYZE :"staging_schema".transcom_event_administative_geographies;
