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
              (SELECT deps FROM cte_deps)
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
              (SELECT deps FROM cte_deps)
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
              (SELECT deps FROM cte_deps)
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
              (SELECT deps FROM cte_deps)
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
              (SELECT deps from cte_deps)
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
              (SELECT deps from cte_deps)
            ) ON CONFLICT DO NOTHING
          ;

      END IF ;

      IF NOT EXISTS ( SELECT 1 FROM data_manager.sources WHERE name = 'NpmrdsTravelTimesExportDb'  )
        THEN

          WITH cte_deps AS (
            SELECT
                array_agg(source_id ORDER BY source_id) AS deps
              FROM data_manager.sources
              WHERE ( name IN (
                  'NpmrdsTravelTimesExportSqlite'
                )
              )
          ) INSERT INTO data_manager.sources(
                name,
                description,
                type,
                display_name,
                source_dependencies
            ) VALUES (
              'NpmrdsTravelTimesExportDb',
              'Database table containing the NPMRDS Travel Times loaded from an NpmrdsTravelTimesExport. Authoritative versions are integrated into the NpmrdsAuthoritativeTravelTimesDb data type.',
              'npmrds_travel_times_export_db',
              'NPMRDS Travel Times Export Database Table',
              (SELECT deps from cte_deps)
            ) ON CONFLICT DO NOTHING
          ;

      END IF ;


      IF NOT EXISTS ( SELECT 1 FROM data_manager.sources WHERE name = 'NpmrdsAuthoritativeTravelTimesDb'  )
        THEN

          INSERT INTO data_manager.sources(
              name,
              description,
              type,
              display_name
          ) VALUES (
            'NpmrdsAuthoritativeTravelTimesDb',
            'Database table containing the authoritative NPMRDS Travel Times. The NPMRDS Authoritative Travel Times Database Table combines many NPMRDS Travel Times Export Database Tables.',
            'npmrds_authoritative_travel_times_db',
            'NPMRDS Authoritative Travel Times Database Table'
          ) ON CONFLICT DO NOTHING ;

          -- The NpmrdsTravelTimesExportDb data sources is a tree.
          --   At the leaves, the source_dependencies are NpmrdsTravelTimesCsv
          --   At the internal nodes, the source_dependencies are NpmrdsTravelTimesExportDb
          UPDATE data_manager.sources
            SET source_dependencies = (
              ARRAY[
                ARRAY[(
                  SELECT
                      source_id
                    FROM data_manager.sources
                    WHERE name = 'NpmrdsTravelTimesExportDb'
                )],
                ARRAY[(
                  SELECT
                      source_id
                    FROM data_manager.sources
                    WHERE name = 'NpmrdsAuthoritativeTravelTimesDb'
                )]
              ]
            )
            WHERE ( name = 'NpmrdsAuthoritativeTravelTimesDb' )
          ;

      END IF ;

    END
  $$
;
