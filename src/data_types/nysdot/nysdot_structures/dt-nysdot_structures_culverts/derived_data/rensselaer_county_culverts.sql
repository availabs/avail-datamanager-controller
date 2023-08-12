/*
  INFO: Open of `/vsizip/NYSDOT_Structures.gdb.zip'
      using driver `OpenFileGDB' successful.

Layer name: vw_LargeCulverts
Geometry: 3D Measured Point
Feature Count: 7988
Extent: (-79.756825, 40.595630) - (-71.912484, 45.006424)
Layer SRS WKT:
GEOGCS["WGS 84",
    DATUM["WGS_1984",
        SPHEROID["WGS 84",6378137,298.257223563,
            AUTHORITY["EPSG","7030"]],
        AUTHORITY["EPSG","6326"]],
    PRIMEM["Greenwich",0,
        AUTHORITY["EPSG","8901"]],
    UNIT["degree",0.0174532925199433,
        AUTHORITY["EPSG","9122"]],
    AUTHORITY["EPSG","4326"]]
FID Column = ObjectID
Geometry Column = Shape
BIN: String (7.0)
LocationLastUpdateDate: DateTime (0.0)
Crossed: String (50.0)
PrimaryOwner: String (4000.0)
PrimaryMaintenance: String (4000.0)
County: String (100.0)
Region: String (50.0)
Residency: String (10.0)
GTMSstructure: String (100.0)
GTMSMaterial: String (100.0)
ConditionRating: Real(Float32) (0.0)
LastInspectionDate: DateTime (0.0)
Route: String (100.0)
ReferenceMarker: String (100.0)
TypeMaxSpanDesign: String (100.0)
YearBuilt: Integer (0.0)
AbutmentType: String (100.0)
StreamBedMaterial: String (100.0)
MaintenanceResponsibility: String (4000.0)
AbutmentHeight: Real (0.0)
CulvertSkew: Real (0.0)
OutToOutWidth: Real (0.0)
NumberOfSpans: Real (0.0)
SpanLength: Real (0.0)
StructureLength: Real (0.0)
GeneralRecommendation: Real (0.0)
REDC: String (50.0)
*/

COPY (
  SELECT
      bin,
      county,
      primary_own AS primary_owner,
      primary_mai AS primary_maintenance,
      condition_r AS condition_rating
    FROM nysdot_structures.culverts
    WHERE ( county = 'RENSSELAER' )
) TO STDOUT WITH CSV HEADER ;
