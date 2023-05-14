SELECT DISTINCT
    c.state,
    extract(YEAR from a.start_date) as year,
    extract(MONTH from a.start_date) as month
  FROM data_manager.views AS a
    INNER JOIN information_schema.tables AS b
      USING (table_schema, table_name)
    INNER JOIN public.fips_codes AS c
      ON (a.geography_version = c.state_code)
  WHERE table_schema = 'npmrds_travel_times_imports'
  ORDER BY 1,2,3;
