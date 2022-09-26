#!/bin/bash

/usr/lib/postgresql/11/bin/pg_dump \
    -hpluto.availabs.org \
    -p5432 \
    -Unpmrds_admin \
    -dnpmrds_production \
    --no-owner \
    --no-privileges \
    --schema-only \
    --schema data_manager \
    --schema _data_manager_admin \
  > data_manager_schema.sql

