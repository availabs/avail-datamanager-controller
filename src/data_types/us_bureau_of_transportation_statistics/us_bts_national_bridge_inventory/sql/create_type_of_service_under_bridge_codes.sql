-- https://www.fhwa.dot.gov/bridge/mtguide.pdf
-- https://fgdl.org/zips/metadata/htm/bts_bridge_jun17.htm
-- Item 52 - Type of Service (under bridge)

BEGIN ;

CREATE TABLE IF NOT EXISTS us_bureau_of_transportation_statistics.type_of_service_under_bridge_codes (
  code          TEXT PRIMARY KEY,
  description   TEXT NOT NULL
);

INSERT INTO us_bureau_of_transportation_statistics.type_of_service_under_bridge_codes
  VALUES 
    ( '1', 'Highway, with or without pedestrian' ),
    ( '2', 'Railroad' ),
    ( '3', 'Pedestrian-bicycle' ),
    ( '4', 'Highway-railroad' ),
    ( '5', 'Waterway' ),
    ( '6', 'Highway-waterway' ),
    ( '7', 'Railroad-waterway' ),
    ( '8', 'Highway-waterway-railroad' ),
    ( '9', 'Relief for waterway' ),
    ( '0', 'Other' )
    
  ON CONFLICT DO NOTHING
;

COMMIT ;
