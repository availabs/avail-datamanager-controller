#!/bin/sh

set -e

gpkg=/data/merged/fp_pct/merged_ble_fp_pcts.gpkg
gdb=/data/merged/fp_pct/merged_ble_fp_pcts.gdb

rm -rf $gdb

# Create a GeoDatabase from the GeoPackage.
ogr2ogr -F OpenFileGDB $gdb $gpkg

# Rename the SHAPE_Length column to SHAPE_Leng to match the specficied schema.
ogrinfo \
  $gdb \
  -sql "ALTER TABLE merged_ble_fp_pcts RENAME COLUMN SHAPE_Length TO SHAPE_Leng"

ogrinfo \
  $gdb \
  -sql "ALTER TABLE merged_ble_fp_pcts RENAME COLUMN FP_AR_ID TO FLD_AR_ID"

# For some reason I have not been able to determine, the UPDATE commands
#   cause ogrinfo to have an exit code of 1. This crashes the script.
# To make sure we truly update ALL the SHAPE_Leng and SHAPE_Area values,
#   we first set all the values to -1, then update using the
#   ST_Perimeter and ST_Area functions. No values should be -1 when done.

set +e

ogrinfo $gdb -dialect sqlite -sql "UPDATE merged_ble_fp_pcts SET SHAPE_Leng=-1"

MAX_SHAPE_Leng=$(
  ogr2ogr \
    -F CSV \
    /vsistdout/ \
    $gdb \
    -sql "SELECT MAX(SHAPE_Leng) FROM merged_ble_fp_pcts" \
  | tail -1
)

if [ "$MAX_SHAPE_Leng" != "-1" ]; then
  echo 'ERROR: Unable to set SHAPE_Leng to -1'
  exit 1
else
  echo 'Set all SHAPE_Leng to -1'
fi

ogrinfo $gdb -dialect sqlite -sql "UPDATE merged_ble_fp_pcts SET SHAPE_Leng=ST_Perimeter(shape)"

MAX_SHAPE_Leng=$(
  ogr2ogr \
    -F CSV \
    /vsistdout/ \
    $gdb \
    -sql "SELECT MAX(SHAPE_Leng) FROM merged_ble_fp_pcts" \
  | tail -1
)

if [ "$MAX_SHAPE_Leng" == "-1" ]; then
  echo 'ERROR: Unable to set SHAPE_Leng to using ST_Perimeter'
  exit 1
else
  echo 'Set all SHAPE_Leng to ST_Perimeter value.'
fi

ogrinfo $gdb -dialect sqlite -sql "UPDATE merged_ble_fp_pcts SET SHAPE_Area=-1"

MAX_SHAPE_Area=$(
  ogr2ogr \
    -F CSV \
    /vsistdout/ \
    $gdb \
    -sql "SELECT MAX(SHAPE_Area) FROM merged_ble_fp_pcts" \
  | tail -1
)

if [ "$MAX_SHAPE_Area" != "-1" ]; then
  echo 'ERROR: Unable to set SHAPE_Area to -1'
  exit 1
else
  echo 'Set all SHAPE_Area to -1'
fi

ogrinfo $gdb -dialect sqlite -sql "UPDATE merged_ble_fp_pcts SET SHAPE_Area=ST_Area(shape)"

MAX_SHAPE_Area=$(
  ogr2ogr \
    -F CSV \
    /vsistdout/ \
    $gdb \
    -sql "SELECT MAX(SHAPE_Area) FROM merged_ble_fp_pcts" \
  | tail -1
)

if [ "$MAX_SHAPE_Area" == "-1" ]; then
  echo 'ERROR: Unable to set SHAPE_Area to using ST_Area'
  exit 1
else
  echo 'Set all SHAPE_Area to ST_Area'
fi

set -e

echo 'DONE'

# Make the output files read-only.
# chmod -R -w $gpkg $gpg
