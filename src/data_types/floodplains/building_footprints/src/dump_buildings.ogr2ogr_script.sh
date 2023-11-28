#!/bin/sh

set -e

CREDS="host='' user='' dbname='' password='' port=''"

OUTF="/data/buildings_in_floodplains.gdb"

ogr2ogr \
  -F OpenFileGDB $OUTF \
  "PG:$CREDS tables=floodplains_pjt.buildings_in_floodplains" \
  -nln buildings_in_floodplains
