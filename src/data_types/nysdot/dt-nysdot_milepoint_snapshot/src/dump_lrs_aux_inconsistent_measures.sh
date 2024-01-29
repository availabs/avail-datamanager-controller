#!/bin/bash

set -e

DERIVED_DATA_DIR=../derived_data
DB_SCHEMA=nysdot_milepoint_2021_etl
VERSION=2021-12-25_MilepointSnapshot

mkdir -p $DERIVED_DATA_DIR

OUTFILE_NAME="${VERSION}.lrs_aux_inconsistent_measures.ndjson.gz"
OUTFILE_PATH="$DERIVED_DATA_DIR/$OUTFILE_NAME"

CREDS="host='127.0.0.1' user='dama_dev_user' dbname='dama_dev_1' password='3eee6405-48e5-4a1e-a6b8-82bfc80b5f17' port='5466'"

psql \
  -d "$CREDS" \
  -c "
    COPY (
      SELECT
          ROW_TO_JSON(t)
        FROM $DB_SCHEMA.qa_lrs_aux_features_with_inconsistent_measures AS t
        WHERE ( ABS(measure_len_diff_feet) > 50)
        ORDER BY ABS(measure_len_diff_feet) DESC
    ) TO STDOUT
  " \
| gzip -9 \
> $OUTFILE_PATH