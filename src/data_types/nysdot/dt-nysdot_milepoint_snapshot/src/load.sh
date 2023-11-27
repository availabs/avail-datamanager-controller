#!/bin/bash

set -e


SOURCE_DATA_NAME=2020-12-23_MilepointSnapshot
SOURCE_DATA_DIR=../initial_data/extracted

GDB_NAME=$SOURCE_DATA_NAME.gdb
GPKG_NAME=$SOURCE_DATA_NAME.gpkg

SCHEMA=nysdot_milepoint_2020

CREDS="host='127.0.0.1' user='dama_dev_user' dbname='dama_dev_1' password='3eee6405-48e5-4a1e-a6b8-82bfc80b5f17' port='5466'"

# Need to CONVERT_TO_LINEAR. Does not work using the PostGIS driver.
if [ ! -f $SOURCE_DATA_DIR/$GPKG_NAME ]
then
  echo 'Creating GPKG.'

  ogr2ogr \
    -nlt CONVERT_TO_LINEAR \
    -nlt PROMOTE_TO_MULTI \
    -F GPKG \
    $SOURCE_DATA_DIR/$GPKG_NAME \
    $SOURCE_DATA_DIR/$GDB_NAME

  chmod -w $SOURCE_DATA_DIR/$GPKG_NAME
fi

psql \
  -d "$CREDS" \
  -c "DROP SCHEMA IF EXISTS $SCHEMA CASCADE;" \
  -c "CREATE SCHEMA IF NOT EXISTS $SCHEMA;"

# NOTE: Added -skipfailures because of the following error
# ERROR 1: Point outside of projection domain
# ERROR 1: Point outside of projection domain
# ERROR 1: Failed to reproject feature 410245 (geometry probably out of source or destination SRS).
# ERROR 1: Terminating translation prematurely after failed
# translation of layer Calibration_Point (use -skipfailures to skip errors)

ogr2ogr \
  -doo "PRELUDE_STATEMENTS=BEGIN;" \
  -nlt PROMOTE_TO_MULTI \
  -F PostgreSQL PG:"$CREDS active_schema=$SCHEMA"\
  $SOURCE_DATA_DIR/$GPKG_NAME \
  -lco GEOMETRY_NAME=wkb_geometry \
  -lco DIM=XY \
  -t_srs EPSG:4326 \
  -preserve_fid \
  -skipfailures \
  -lco FID=ogc_fid \
  --config PGSQL_OGR_FID ogc_fid \
  -doo CLOSING_STATEMENTS=COMMIT
