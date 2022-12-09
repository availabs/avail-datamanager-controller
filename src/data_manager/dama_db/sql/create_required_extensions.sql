CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- https://gis.stackexchange.com/a/341598
DO
  LANGUAGE plpgsql
  $$
    BEGIN
      IF ( SELECT PostGIS_Version() >= '3' )
        THEN
          CREATE EXTENSION IF NOT EXISTS postgis_raster;
      END IF ;
    END
  $$
;
