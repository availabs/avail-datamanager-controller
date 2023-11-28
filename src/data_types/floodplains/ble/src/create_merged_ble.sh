#!/bin/sh

set -e

merged_ble_fld_haz_ar_gdb=/data/merged/merged_ble_fld_haz_ar.gdb
merged_ble_fp_pcts_gdb=/data/merged/fp_pct/merged_ble_fp_pcts.gdb

merged_ble_gdb=/data/merged/merged_ble.gdb

rm -rf $merged_ble_gdb

ogrmerge.py \
  -single \
  -f OpenFileGDB \
  -o $merged_ble_gdb \
  $merged_ble_fld_haz_ar_gdb \
  $merged_ble_fp_pcts_gdb \
  -nln 'merged_ble'
