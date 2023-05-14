/*
  In ../authoritative_npmrds_tables.ts used the wrong end_date for NY.
    Used "2022-05-31". Should have been "2023-04-30".
*/

SELECT
    view_id,
    table_schema,
    table_name
  FROM data_manager.sources AS a
    INNER JOIN data_manager.views AS b
      USING (source_id)
    LEFT OUTER JOIN (
        SELECT
            view_dependencies
          FROM data_manager.sources AS x
            INNER JOIN data_manager.views AS y
              USING (source_id)
          WHERE (
            ( x.name = 'NpmrdsTravelTimes' )
            AND
            ( y.active_start_timestamp IS NOT NULL )
            AND
            ( y.active_end_timestamp IS NULL )
          )
      ) AS c ON ( b.view_id = ANY(c.view_dependencies) )
  WHERE (
    ( a.name = 'NpmrdsTravelTimesImports' )
    AND
    ( b.geography_version = '36' )
    AND
    ( c.view_dependencies IS NULL )
  )
  ORDER BY 2, 3
;

