#!/bin/sh

set -e

gpkg=/data/merged/merged_ble_fld_haz_ar.gpkg
gdb=/data/merged/merged_ble_fld_haz_ar.gdb

rm -rf $gdb

# Create a GeoDatabase from the GeoPackage.
ogr2ogr -F OpenFileGDB $gdb $gpkg -nln merged_ble_fld_haz_ar

# Rename the SHAPE_Length column to SHAPE_Leng to match the specficied schema.
ogrinfo \
  $gdb \
  -sql "ALTER TABLE merged_ble_fld_haz_ar RENAME COLUMN SHAPE_Length TO SHAPE_Leng"

set +e

ogrinfo $gdb -dialect sqlite -sql "UPDATE merged_ble_fld_haz_ar SET SHAPE_Leng=-1"

MAX_SHAPE_Leng=$(
  ogr2ogr \
    -F CSV \
    /vsistdout/ \
    $gdb \
    -sql "SELECT MAX(SHAPE_Leng) FROM merged_ble_fld_haz_ar" \
  | tail -1
)

if [ "$MAX_SHAPE_Leng" != "-1" ]; then
  echo 'ERROR: Unable to set SHAPE_Leng to -1'
  exit 1
else
  echo 'Set all SHAPE_Leng to -1'
fi

ogrinfo $gdb -dialect sqlite -sql "UPDATE merged_ble_fld_haz_ar SET SHAPE_Leng=ST_Perimeter(shape)"

MAX_SHAPE_Leng=$(
  ogr2ogr \
    -F CSV \
    /vsistdout/ \
    $gdb \
    -sql "SELECT MAX(SHAPE_Leng) FROM merged_ble_fld_haz_ar" \
  | tail -1
)

if [ "$MAX_SHAPE_Leng" == "-1" ]; then
  echo 'ERROR: Unable to set SHAPE_Leng to using ST_Perimeter'
  exit 1
else
  echo 'Set all SHAPE_Leng to ST_Perimeter value.'
fi

ogrinfo $gdb -dialect sqlite -sql "UPDATE merged_ble_fld_haz_ar SET SHAPE_Area=-1"

MAX_SHAPE_Area=$(
  ogr2ogr \
    -F CSV \
    /vsistdout/ \
    $gdb \
    -sql "SELECT MAX(SHAPE_Area) FROM merged_ble_fld_haz_ar" \
  | tail -1
)

if [ "$MAX_SHAPE_Area" != "-1" ]; then
  echo 'ERROR: Unable to set SHAPE_Area to -1'
  exit 1
else
  echo 'Set all SHAPE_Area to -1'
fi

ogrinfo $gdb -dialect sqlite -sql "UPDATE merged_ble_fld_haz_ar SET SHAPE_Area=ST_Area(shape)"

MAX_SHAPE_Area=$(
  ogr2ogr \
    -F CSV \
    /vsistdout/ \
    $gdb \
    -sql "SELECT MAX(SHAPE_Area) FROM merged_ble_fld_haz_ar" \
  | tail -1
)

if [ "$MAX_SHAPE_Area" == "-1" ]; then
  echo 'ERROR: Unable to set SHAPE_Area to using ST_Area'
  exit 1
else
  echo 'Set all SHAPE_Area to ST_Area'
fi

set -e



# Make the output files read-only.
chmod -R -w $gpkg $gpg
