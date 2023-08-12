DROP TABLE IF EXISTS osm.roadway_nhd_flowline_crossing ;

CREATE TABLE osm.roadway_nhd_flowline_crossing
  AS
    SELECT
        a.osm_id,
        b.permanent_identifier AS nhd_flowline_permanent_identifier,
        ST_Intersection(
          a.wkb_geometry,
          b.wkb_geometry
        ) AS wkb_geometry
      FROM osm.roadways AS a
        INNER JOIN usgs_national_hydrography_dataset.flowline AS b
          ON (
            ST_Intersects(
              a.wkb_geometry,
              b.wkb_geometry
            )
          )
;
