/*
  Would be preferable to group bridges' ways into a single entity.
  https://gis.stackexchange.com/a/94228
  NOTE: It is not guaranteed that Way order across the bridge is consistent.
*/

CREATE OR REPLACE VIEW osm.roadway_bridges
  AS
    SELECT
        *
      FROM osm.roadways
      WHERE ( bridge IS NOT NULL )
;
