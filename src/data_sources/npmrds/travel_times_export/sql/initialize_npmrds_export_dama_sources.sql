-- Using a DO block because ON CONFLICT DO NOTHING still increments the source_id SEQUENCE.
DO
  LANGUAGE plpgsql
  $$
    BEGIN

      IF NOT EXISTS ( SELECT 1 FROM data_manager.sources WHERE name = 'NpmrdsTravelTimesExport' )
        THEN

          INSERT INTO data_manager.sources(
              name,
              description,
              type,
              display_name
          ) VALUES (
            'NpmrdsTravelTimesExport',
            'Raw RITIS NPMRDS Travel Times Export ZIP archives as downloaded from RITIS. Comprised of the all vehicle, passenger vehicle, and freight truck exports.',
            'npmrds_travel_times_export',
            'NPMRDS Travel Times Export'
          ) ;

      END IF ;

      IF NOT EXISTS ( SELECT 1 FROM data_manager.sources WHERE name = 'NpmrdsAllVehTravelTimesExport' )
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
              'NpmrdsAllVehTravelTimesExport',
              'Raw RITIS NPMRDS all vehicles travel times export ZIP archive downloaded as part of a RITIS NPMRDS Travel Times Export. The ZIP archive includes the all vehicle travel times CSV and a TMC_Identification CSV that provides road segment metadata.',
              'npmrds_data_source_travel_times_export',
              'Raw NPMRDS All Vehicles Travel Times Export',
              (SELECT ARRAY[deps] FROM cte_deps)
            ) ON CONFLICT DO NOTHING
          ;

      END IF ;

      IF NOT EXISTS ( SELECT 1 FROM data_manager.sources WHERE name = 'NpmrdsPassVehTravelTimesExport' )
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
              'NpmrdsPassVehTravelTimesExport',
              'Raw RITIS NPMRDS passenger vehicles travel times export ZIP archive downloaded as part of a RITIS NPMRDS Travel Times Export. The ZIP archive includes the passenger vehicle travel times CSV and a TMC_Identification CSV that provides road segment metadata.',
              'npmrds_data_source_travel_times_export',
              'Raw NPMRDS Passenger Vehicles Travel Times Export',
              (SELECT ARRAY[deps] FROM cte_deps)
            ) ON CONFLICT DO NOTHING
          ;

      END IF ;

      IF NOT EXISTS ( SELECT 1 FROM data_manager.sources WHERE name = 'NpmrdsFrgtTrkTravelTimesExport' )
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
              'NpmrdsFrgtTrkTravelTimesExport',
              'Raw RITIS NPMRDS freight truck travel times export ZIP archive downloaded as part of a RITIS NPMRDS Travel Times Export. The ZIP archive includes the freight trucks travel times CSV and a TMC_Identification CSV that provides road segment metadata.',
              'npmrds_data_source_travel_times_export',
              'Raw NPMRDS Passenger Vehicles Travel Times Export',
              (SELECT ARRAY[deps] FROM cte_deps)
            ) ON CONFLICT DO NOTHING
          ;

      END IF ;

      IF NOT EXISTS ( SELECT 1 FROM data_manager.sources WHERE name = 'NpmrdsTmcIdentificationCsv'  )
        THEN

          WITH cte_deps AS (
            SELECT
                array_agg(source_id ORDER BY source_id) AS deps
              FROM data_manager.sources
              WHERE ( name = 'NpmrdsAllVehTravelTimesExport' )
          )
            INSERT INTO data_manager.sources(
                name,
                description,
                type,
                display_name,
                source_dependencies
            ) VALUES (
              'NpmrdsTmcIdentificationCsv',
              'Raw NPMRDS TMC Identification CSV included in the Raw NPMRDS All Vehicles Travel Times Export. This CSV contains metadata describing the TMC segments included in the export.',
              'csv',
              'NPMRDS TMC Identification CSV',
              (SELECT ARRAY[deps] FROM cte_deps)
            ) ON CONFLICT DO NOTHING
          ;

      END IF ;

      IF NOT EXISTS ( SELECT 1 FROM data_manager.sources WHERE name = 'NpmrdsTravelTimesCsv'  )
        THEN

          WITH cte_deps AS (
            SELECT
                array_agg(source_id ORDER BY source_id) AS deps
              FROM data_manager.sources
              WHERE ( name IN (
                  'NpmrdsAllVehTravelTimesExport',
                  'NpmrdsPassVehTravelTimesExport',
                  'NpmrdsFrgtTrkTravelTimesExport'
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
              'npmrds_travel_times_csv',
              'NPMRDS Travel Times CSV',
              (SELECT ARRAY[deps] from cte_deps)
            ) ON CONFLICT DO NOTHING
          ;

      END IF ;

      IF NOT EXISTS ( SELECT 1 FROM data_manager.sources WHERE name = 'NpmrdsTravelTimesExportSqlite'  )
        THEN

          WITH cte_deps AS (
            SELECT
                array_agg(source_id ORDER BY source_id) AS deps
              FROM data_manager.sources
              WHERE ( name IN (
                  'NpmrdsAllVehTravelTimesExport',
                  'NpmrdsPassVehTravelTimesExport',
                  'NpmrdsFrgtTrkTravelTimesExport',
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
              'NpmrdsTravelTimesExportSqlite',
              'NPMRDS Travel Times SQLite DB that contains two tables. The "npmrds" table joins the NPMRDS Travel Times Export all vehicle, passenger vehicle, and freight truck travel times CSVs. The "tmc_idenification" table contains the TMC identification CSV included in the export. This file is an intermediary product of the ETL process and is preserved for analysis purposes.',
              'npmrds_travel_times_export_sqlite',
              'NPMRDS Travel Times Export SQLite',
              (SELECT ARRAY[deps] from cte_deps)
            ) ON CONFLICT DO NOTHING
          ;

      END IF ;

      IF NOT EXISTS ( SELECT 1 FROM data_manager.sources WHERE name = 'NpmrdsTravelTimesDb'  )
        THEN

          INSERT INTO data_manager.sources(
              name,
              description,
              type,
              display_name
          ) VALUES (
            'NpmrdsTravelTimesDb',
            'Database table containing the NPMRDS Travel Times.',
            'npmrds_travel_times_db',
            'NPMRDS Travel Times Database Table'
          ) ON CONFLICT DO NOTHING ;

          -- The NpmrdsTravelTimesDb data sources is a tree. As trees are recursive data structures,
          --   At the leaves, the source_dependencies are NpmrdsTravelTimesCsv
          --   At the internal nodes, the source_dependencies are NpmrdsTravelTimesDb
          UPDATE data_manager.sources
            SET source_dependencies = (
              ARRAY[
                ARRAY[(
                  SELECT
                      source_id
                    FROM data_manager.sources
                    WHERE name = 'NpmrdsTravelTimesCsv'
                )],
                ARRAY[(
                  SELECT
                      source_id
                    FROM data_manager.sources
                    WHERE name = 'NpmrdsTravelTimesDb'
                )]
              ]
            )
            WHERE ( name = 'NpmrdsTravelTimesDb' )
          ;


      END IF ;


    END
  $$
;
