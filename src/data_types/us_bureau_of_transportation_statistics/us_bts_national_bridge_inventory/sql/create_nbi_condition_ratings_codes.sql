BEGIN ;

CREATE TABLE IF NOT EXISTS us_bureau_of_transportation_statistics.condition_ratings_codes (
  code                TEXT PRIMARY KEY,
  short_description   TEXT NOT NULL,
  long_description    TEXT
);

INSERT INTO us_bureau_of_transportation_statistics.condition_ratings_codes
  VALUES

    ( 'N',  'NOT APPLICABLE', NULL ),
    ( '9',  'EXCELLENT CONDITION', NULL),
    ( '8',  'VERY GOOD CONDITION', 'no problems noted' ),
    ( '7',  'GOOD CONDITION', 'some minor problems' ),
    ( '6',  'SATISFACTORY CONDITION', 'structural elements show some minor deterioration' ),
    ( '5',  'FAIR CONDITION', 'all primary structural elements are sound but may have minor section loss, cracking, spalling or scour' ),
    ( '4',  'POOR CONDITION', 'advanced section loss, deterioration, spalling or scour' ),
    ( '3',  'SERIOUS CONDITION', 'loss of section, deterioration, spalling or scour have seriously affected primary structural components. Local failures are possible. Fatigue cracks in steel or shear cracks in concrete may be present' ),
    ( '2',  'CRITICAL CONDITION', 'advanced deterioration of primary structural elements. Fatigue cracks in steel or shear cracks in concrete may be present or scour may have removed substructure support. Unless closely monitored it may be necessary to close the bridge until corrective action is taken' ),
    ( '1',  '"IMMINENT" FAILURE CONDITION', 'major deterioration or section loss present in critical structural components or obvious vertical or horizontal movement affecting structure stability. Bridge is closed to traffic but corrective action may put back in light service' ),
    ( '0',  'FAILED CONDITION', 'out of service - beyond corrective action' )

  ON CONFLICT DO NOTHING
;

COMMIT ;
