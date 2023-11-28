#!/bin/bash

set -e

CREDS="host='' user='' dbname='' password='' port=''"

OUTF="buildings_in_floodplains.gdb"

rm -rf $OUTF

docker \
  run \
  --rm \
  -it \
  -v ${PWD}:/data \
  ghcr.io/osgeo/gdal:alpine-normal-3.7.2 \
  /data/dump_buildings.ogr2ogr_script.sh

sudo chown -R avail:avail $OUTF

chmod -R -w $OUTF
