COPY (
  SELECT
      bin,
      county,
      primary_own AS primary_owner,
      primary_mai AS primary_maintenance,
      condition_r AS condition_rating
    FROM nysdot_structures.bridges
    WHERE ( county = 'ALBANY' )
) TO STDOUT WITH CSV HEADER ;
