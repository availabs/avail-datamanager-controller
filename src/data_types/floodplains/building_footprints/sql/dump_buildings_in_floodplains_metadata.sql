COPY (
  SELECT
      *
    FROM floodplains.buildings_in_floodplains_metadata
) TO STDOUT WITH CSV HEADER ;
