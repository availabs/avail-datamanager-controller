-- Using a DO block because ON CONFLICT DO NOTHING still increments the source_id SEQUENCE.
DO
  LANGUAGE plpgsql
  $$
    DECLARE
      existing_sources  TEXT[] ;

    BEGIN

      SELECT
          array_agg(name)
        FROM data_manager.sources
        WHERE ( name IN (
            'NpmrdsTravelTimesExport',
            'NpmrdsAllVehicleTravelTimesCsv',
            'NpmrdsPassVehicleTravelTimesCsv',
            'NpmrdsTmcIdentificationCsv',
            'NpmrdsFreightTruckTravelTimesCsv',
            'NpmrdsTravelTimesCsv',
            'NpmrdsTravelTimesSqlite'
          )
        )
      INTO existing_sources ;

      IF NOT ( 'NpmrdsTravelTimesExport' = ANY(existing_sources) )
        THEN

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

      END IF ;

      IF NOT ( 'NpmrdsAllVehicleTravelTimesCsv' = ANY(existing_sources) )
        THEN

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
                source_dependencies
            ) VALUES (
              'NpmrdsAllVehicleTravelTimesCsv',
              'Raw NPMRDS all vehicles travel times CSV included in a RITIS NPMRDS Travel Times Export.',
              'csv',
              'Raw NPMRDS All Vehicles Travel Times CSV',
              (SELECT deps FROM cte_deps)
            ) ON CONFLICT DO NOTHING
          ;

      END IF ;

      IF NOT ( 'NpmrdsPassVehicleTravelTimesCsv' = ANY(existing_sources) )
        THEN

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
                source_dependencies
            ) VALUES (
              'NpmrdsPassVehicleTravelTimesCsv',
              'Raw NPMRDS passenger vehicle travel times CSV included in a RITIS NPMRDS Travel Times Export.',
              'csv',
              'Raw NPMRDS Passenger Vehicles Travel Times CSV',
              (SELECT deps FROM cte_deps)
            ) ON CONFLICT DO NOTHING
          ;

      END IF ;

      IF NOT ( 'NpmrdsFreightTruckTravelTimesCsv' = ANY(existing_sources) )
        THEN

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
                source_dependencies
            ) VALUES (
              'NpmrdsFreightTruckTravelTimesCsv',
              'Raw NPMRDS freight truck travel times CSV included in a RITIS NPMRDS Travel Times Export.',
              'csv',
              'Raw NPMRDS Freight Truck Travel Times CSV',
              (SELECT deps FROM cte_deps)
            ) ON CONFLICT DO NOTHING
          ;

      END IF ;

      IF NOT ( 'NpmrdsTmcIdentificationCsv' = ANY(existing_sources) )
        THEN

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
                source_dependencies
            ) VALUES (
              'NpmrdsTmcIdentificationCsv',
              'Raw NPMRDS TMC Identification CSV included in a RITIS NPMRDS Travel Times Export. This CSV contains metadata describing the TMC segments included in the export.',
              'csv',
              'NPMRDS TMC Identification CSV',
              (SELECT deps FROM cte_deps)
            ) ON CONFLICT DO NOTHING
          ;

      END IF ;

      IF NOT ( 'NpmrdsTravelTimesCsv' = ANY(existing_sources) )
        THEN

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
                source_dependencies
            ) VALUES (
              'NpmrdsTravelTimesCsv',
              'NPMRDS Travel Times CSV that joins the all vehicle, passenger vehicle, and freight truck travel times CSVs on (TMC, date, epoch).',
              'csv',
              'NPMRDS Travel Times CSV',
              (SELECT deps from cte_deps)
            ) ON CONFLICT DO NOTHING
          ;

      END IF ;

      IF NOT ( 'NpmrdsTravelTimesSqlite' = ANY(existing_sources) )
        THEN

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
                source_dependencies
            ) VALUES (
              'NpmrdsTravelTimesSqlite',
              'NPMRDS Travel Times SQLite DB that contains a table joining the all vehicle, passenger vehicle, and freight truck travel times CSVs, and a table containing the TMC identification CSV.',
              'sqlite',
              'NPMRDS Travel Times SQLite',
              (SELECT deps from cte_deps)
            ) ON CONFLICT DO NOTHING
          ;

      END IF ;

    END
  $$
;





