SELECT NOT EXISTS(
  SELECT
      *
    FROM (
      SELECT
          *
        FROM floodplains_pjt.buildings_in_floodplains_metadata
      EXCEPT ALL
      SELECT
          *
        FROM floodplains.buildings_in_floodplains_metadata
    ) AS a
  UNION ALL
  SELECT
      *
    FROM (
      SELECT
          *
        FROM floodplains.buildings_in_floodplains_metadata
      EXCEPT ALL
      SELECT
          *
        FROM floodplains_pjt.buildings_in_floodplains_metadata
    ) AS a
) AS passed ;

/*
 passed 
--------
 t
(1 row)
*/
