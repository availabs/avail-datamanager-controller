-- https://www.fhwa.dot.gov/bridge/mtguide.pdf
-- https://fgdl.org/zips/metadata/htm/bts_bridge_jun17.htm
-- Item 54 - Minimum Vertical Underclearance

CREATE TABLE IF NOT EXISTS
  us_bureau_of_transportation_statistics.minimum_vertical_underclearance_reference_feature_codes (
    code          TEXT PRIMARY KEY,
    description   TEXT NOT NULL
  );

INSERT INTO us_bureau_of_transportation_statistics.government_agency_ownership_codes
  VALUES 
    ( 'H',  'Highway beneath structure'         ),
    ( 'R',  'Railroad beneath structure'        ),
    ( 'N',  'Feature not a highway or railroad' )
  ON CONFLICT DO NOTHING
;
