BEGIN ;

CREATE TABLE IF NOT EXISTS us_bureau_of_transportation_statistics.government_agency_ownership_classifications (
  code            TEXT PRIMARY KEY,
  classification  TEXT NOT NULL
);

INSERT INTO us_bureau_of_transportation_statistics.government_agency_ownership_classifications
  VALUES 
     ( '01', 'State' ),
     ( '02', 'County' ),
     ( '03', 'Municipal' ),
     ( '04', 'Municipal' ),
     ( '11', 'State' ),
     ( '12', 'Municipal' ),
     ( '21', 'State' ),
     ( '25', 'Municipal' ),
     ( '26', 'Other' ),
     ( '27', 'Railroad' ),
     ( '31', 'State' ),
     ( '32', 'Municipal' ),
     ( '56', 'Federal' ),
     ( '60', 'Federal' ),
     ( '61', 'Native' ),
     ( '62', 'Native' ),
     ( '63', 'Federal' ),
     ( '64', 'Federal' ),
     ( '66', 'Federal' ),
     ( '67', 'Other' ),
     ( '68', 'Federal' ),
     ( '69', 'Federal' ),
     ( '70', 'Federal' ),
     ( '71', 'Federal' ),
     ( '72', 'Federal' ),
     ( '73', 'Federal' ),
     ( '74', 'Federal' ),
     ( '75', 'Federal' ),
     ( '76', 'Other' ),
     ( '80', 'Other' )
  ON CONFLICT DO NOTHING
;

COMMIT ;
