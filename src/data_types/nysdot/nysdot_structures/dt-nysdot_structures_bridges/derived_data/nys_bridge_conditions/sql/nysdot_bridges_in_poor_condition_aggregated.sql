COPY (
  SELECT
      *
    FROM nysdot_structures.nysdot_bridges_in_poor_condition_aggregated
) TO STDOUT WITH CSV HEADER
;
