COPY (
  SELECT
      county,
      primary_own AS primary_owner,
      COUNT(1) AS total_culverts
    FROM nysdot_structures.culverts
  GROUP BY 1,2
  ORDER BY 1,3 DESC
) TO STDOUT WITH CSV HEADER ;
