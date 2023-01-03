INSERT INTO data_manager.sources(
    name,
    description,
    type,
    display_name
) VALUES (
  'NpmrdsTravelTimesExport',
  'Raw NPMRDS Travel Times Export ZIP archives as downloaded from RITIS',
  'npmrds_travel_times_export',
  'NPMRDS Travel Times Export'
) ON CONFLICT DO NOTHING ;


WITH cte_deps AS (
  SELECT
      array_agg(source_id ORDER BY source_id) AS deps
    FROM data_manager.sources
    WHERE ( name = 'NpmrdsTravelTimesExport' )
)
  INSERT INTO data_manager.sources(
      name,
      description,
      type,
      display_name,
      dependencies
  ) VALUES (
    'NpmrdsAllVehicleTravelTimesCsv',
    'Raw NPMRDS all vehicles travel times CSV included in a RITIS NPMRDS Travel Times Export.',
    'csv',
    'Raw NPMRDS All Vehicles Travel Times CSV',
    (SELECT deps FROM cte_deps)
  ) ON CONFLICT DO NOTHING
;


WITH cte_deps AS (
  SELECT
      array_agg(source_id ORDER BY source_id) AS deps
    FROM data_manager.sources
    WHERE ( name = 'NpmrdsTravelTimesExport' )
)
  INSERT INTO data_manager.sources(
      name,
      description,
      type,
      display_name,
      dependencies
  ) VALUES (
    'NpmrdsPassVehicleTravelTimesCsv',
    'Raw NPMRDS passenger vehicle travel times CSV included in a RITIS NPMRDS Travel Times Export.',
    'csv',
    'Raw NPMRDS Passenger Vehicles Travel Times CSV',
    (SELECT deps FROM cte_deps)
  ) ON CONFLICT DO NOTHING
;

WITH cte_deps AS (
  SELECT
      array_agg(source_id ORDER BY source_id) AS deps
    FROM data_manager.sources
    WHERE ( name = 'NpmrdsTravelTimesExport' )
)
  INSERT INTO data_manager.sources(
      name,
      description,
      type,
      display_name,
      dependencies
  ) VALUES (
    'NpmrdsTmcIdentificationCsv',
    'Raw NPMRDS TMC Identification CSV included in a RITIS NPMRDS Travel Times Export. This CSV contains metadata describing the TMC segments included in the export.',
    'csv',
    'NPMRDS TMC Identification CSV',
    (SELECT deps FROM cte_deps)
  ) ON CONFLICT DO NOTHING
;

WITH cte_deps AS (
  SELECT
      array_agg(source_id ORDER BY source_id) AS deps
    FROM data_manager.sources
    WHERE ( name = 'NpmrdsTravelTimesExport' )
)
  INSERT INTO data_manager.sources(
      name,
      description,
      type,
      display_name,
      dependencies
  ) VALUES (
    'NpmrdsFreightTruckTravelTimesCsv',
    'Raw NPMRDS freight truck travel times CSV included in a RITIS NPMRDS Travel Times Export.',
    'csv',
    'Raw NPMRDS Freight Truck Travel Times CSV',
    (SELECT deps FROM cte_deps)
  ) ON CONFLICT DO NOTHING
;

WITH cte_deps AS (
  SELECT
      array_agg(source_id ORDER BY source_id) AS deps
    FROM data_manager.sources
    WHERE ( name IN (
        'NpmrdsAllVehicleTravelTimesCsv',
        'NpmrdsPassVehicleTravelTimesCsv',
        'NpmrdsFreightTruckTravelTimesCsv'
      )
    )
)
  INSERT INTO data_manager.sources(
      name,
      description,
      type,
      display_name,
      dependencies
  ) VALUES (
    'NpmrdsTravelTimesCsv',
    'NPMRDS Travel Times CSV that joins the all vehicle, passenger vehicle, and freight truck travel times CSVs on (TMC, date, epoch).',
    'csv',
    'NPMRDS Travel Times CSV',
    (SELECT deps from cte_deps)
  ) ON CONFLICT DO NOTHING
;


WITH cte_deps AS (
  SELECT
      array_agg(source_id ORDER BY source_id) AS deps
    FROM data_manager.sources
    WHERE ( name IN (
        'NpmrdsAllVehicleTravelTimesCsv',
        'NpmrdsPassVehicleTravelTimesCsv',
        'NpmrdsFreightTruckTravelTimesCsv',
        'NpmrdsTmcIdentificationCsv'
      )
    )
) INSERT INTO data_manager.sources(
      name,
      description,
      type,
      display_name,
      dependencies
  ) VALUES (
    'NpmrdsTravelTimesSqlite',
    'NPMRDS Travel Times SQLite DB that contains a table joining the all vehicle, passenger vehicle, and freight truck travel times CSVs, and a table containing the TMC identification CSV.',
    'sqlite',
    'NPMRDS Travel Times SQLite',
    (SELECT deps from cte_deps)
  ) ON CONFLICT DO NOTHING
;
