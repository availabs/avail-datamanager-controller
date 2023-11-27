#!/bin/bash

set -e

DERIVED_DATA_DIR=../derived_data
DB_SCHEMA=nysdot_milepoint_2021_etl
VERSION=2021-12-25_MilepointSnapshot

GPKG_NAME="${VERSION}.basemap.$(date +%Y%m%dT%H%M%S).gpkg"

GPKG_PATH="${DERIVED_DATA_DIR}/${GPKG_NAME}"

rm -f $GPKG_PATH

mkdir -p $DERIVED_DATA_DIR

CREDS="host='127.0.0.1' user='dama_dev_user' dbname='dama_dev_1' password='3eee6405-48e5-4a1e-a6b8-82bfc80b5f17' port='5466'"

ogr2ogr \
  -F GPKG "$GPKG_PATH" \
  PG:"$CREDS" \
  "$DB_SCHEMA.paritioned_lrs_milepoint_linestrings"

chmod -w "$GPKG_PATH"
