/* eslint-disable max-len */

import { Graph, alg as GraphAlgorithms } from "graphlib";
import _ from "lodash";

export enum NpmrdsDataSources {
  // The Raw RITIS NPMRDS Travel Times Export download with its three ZIP archives.
  NpmrdsTravelTimesExportRitis = "NpmrdsTravelTimesExportRitis",

  // The ETL output
  NpmrdsTravelTimesExportEtl = "NpmrdsTravelTimesExportEtl",

  // The NPMRDS Travel Times Postgres DB Tables
  NpmrdsTravelTimesImp = "NpmrdsTravelTimesImp",
  NpmrdsTravelTimes = "NpmrdsTravelTimes",

  // The NPMRDS TMC Identification Postgres DB Tables
  NpmrdsTmcIdentificationImp = "NpmrdsTmcIdentificationImp",
  NpmrdsTmcIdentification = "NpmrdsTmcIdentification",
}

export enum NpmrdsTravelTimesExportRitisElements {
  NpmrdsAllVehiclesTravelTimesExport = "NpmrdsAllVehiclesTravelTimesExport",
  NpmrdsPassengerVehiclesTravelTimesExport = "NpmrdsPassengerVehiclesTravelTimesExport",
  NpmrdsFreightTrucksTravelTimesExport = "NpmrdsFreightTrucksTravelTimesExport",
}

export enum NpmrdsTravelTimesExportEtlElements {
  NpmrdsTravelTimesExportSqlite = "NpmrdsTravelTimesExportSqlite",
  NpmrdsTmcIdentificationCsv = "NpmrdsTmcIdentificationCsv",
  NpmrdsTravelTimesCsv = "NpmrdsTravelTimesCsv",
}

export enum NpmrdsDatabaseSchemas {
  NpmrdsTravelTimesImp = "npmrds_travel_times_imports",

  NpmrdsTravelTimes = "npmrds_travel_times",

  NpmrdsTmcIdentificationImp = "npmrds_tmc_identification_imp",

  NpmrdsTmcIdentification = "npmrds_tmc_identification",
}

// NOTE:  These the DataSourceMeta property values could become stale
//          if they are later updated.
//        Therefore, these values are for initialization.
//            Current values should be queried from the DB.
export const npmrdsDataSourcesInitialMetadataByName = {
  [NpmrdsDataSources.NpmrdsTravelTimesExportRitis]: {
    description:
      "Raw RITIS NPMRDS Travel Times Export ZIP archives as downloaded from RITIS. Comprised of the all vehicle, passenger vehicle, and freight truck exports.",
    type: "npmrds_travel_times_export_ritis",
    display_name: "NPMRDS Travel Times Export (RITIS)",
    source_dependencies_names: null,
    metadata: {
      elements: {
        [NpmrdsTravelTimesExportRitisElements.NpmrdsAllVehiclesTravelTimesExport]:
          {
            description:
              "Raw RITIS NPMRDS all vehicles travel times export ZIP archive downloaded as part of a RITIS NPMRDS Travel Times Export. The ZIP archive includes the all vehicle travel times CSV and a TMC_Identification CSV that provides road segment metadata.",
            type: "npmrds_data_source_travel_times_export",
            display_name: "Raw NPMRDS All Vehicles Travel Times Export",
          },

        [NpmrdsTravelTimesExportRitisElements.NpmrdsPassengerVehiclesTravelTimesExport]:
          {
            description:
              "Raw RITIS NPMRDS passenger vehicles travel times export ZIP archive downloaded as part of a RITIS NPMRDS Travel Times Export. The ZIP archive includes the passenger vehicle travel times CSV and a TMC_Identification CSV that provides road segment metadata.",
            type: "npmrds_data_source_travel_times_export",
            display_name: "Raw NPMRDS Passenger Vehicles Travel Times Export",
          },

        [NpmrdsTravelTimesExportRitisElements.NpmrdsFreightTrucksTravelTimesExport]:
          {
            description:
              "Raw RITIS NPMRDS freight truck travel times export ZIP archive downloaded as part of a RITIS NPMRDS Travel Times Export. The ZIP archive includes the freight trucks travel times CSV and a TMC_Identification CSV that provides road segment metadata.",
            type: "npmrds_data_source_travel_times_export",
            display_name: "Raw NPMRDS Freight Truck Travel Times Export",
          },
      },
    },
  },

  [NpmrdsDataSources.NpmrdsTravelTimesExportEtl]: {
    description:
      "Output of the ETL processing performed on the NpmrdsTravelTimesExportRitis data source.",
    type: "npmrds_travel_times_export_etl",
    display_name: "NPMRDS Travel Times Export (ETL Output)",
    source_dependencies_names: [NpmrdsDataSources.NpmrdsTravelTimesExportRitis],
    metadata: {
      elements: {
        [NpmrdsTravelTimesExportEtlElements.NpmrdsTravelTimesExportSqlite]: {
          description:
            'NPMRDS Travel Times SQLite DB that contains two tables. The "npmrds" table joins the NPMRDS Travel Times Export all vehicle, passenger vehicle, and freight truck travel times CSVs. The "tmc_idenification" table contains the TMC identification CSV included in the export. This file is an intermediary product of the ETL process and is preserved for analysis purposes.',
          type: "npmrds_travel_times_export_sqlite",
          display_name: "NPMRDS Travel Times Export SQLite",
        },

        [NpmrdsTravelTimesExportEtlElements.NpmrdsTmcIdentificationCsv]: {
          description:
            "Raw NPMRDS TMC Identification CSV included in the Raw NPMRDS All Vehicles Travel Times Export. This CSV contains metadata describing the TMC segments included in the export.",
          type: "npmrds_tmc_identification_csv",
          display_name: "NPMRDS TMC Identification CSV",
          source_dependencies_names: [
            NpmrdsDataSources.NpmrdsTravelTimesExportRitis,
          ],
        },

        [NpmrdsTravelTimesExportEtlElements.NpmrdsTravelTimesCsv]: {
          description:
            "NPMRDS Travel Times CSV that joins the all vehicle, passenger vehicle, and freight truck travel times CSVs on (TMC, date, epoch).",
          type: "npmrds_travel_times_csv",
          display_name: "NPMRDS Travel Times CSV",
          source_dependencies_names: [
            NpmrdsDataSources.NpmrdsTravelTimesExportRitis,
          ],
        },
      },
    },
  },

  [NpmrdsDataSources.NpmrdsTravelTimesImp]: {
    description:
      "Database table containing the NPMRDS Travel Times imported into the database from an NpmrdsTravelTimesExportRitis. These imports are non-authoritative until made authoritative after QA. Authoritative versions are integrated into the NpmrdsTravelTimes data type.",
    type: "npmrds_travel_times_imp",
    display_name: "NPMRDS Travel Times Import",
    source_dependencies_names: [NpmrdsDataSources.NpmrdsTravelTimesExportEtl],
  },

  [NpmrdsDataSources.NpmrdsTravelTimes]: {
    description:
      "Database table containing the authoritative NPMRDS Travel Times. The NPMRDS Authoritative Travel Times Database Table combines many NPMRDS Travel Times Imports.",
    type: "npmrds_travel_times",
    display_name: "NPMRDS Travel Times",
    source_dependencies_names: [NpmrdsDataSources.NpmrdsTravelTimesImp],
  },

  [NpmrdsDataSources.NpmrdsTmcIdentificationImp]: {
    description:
      "Database table containing the imported raw NPMRDS TMC Identification CSV. This table contains metadata describing the TMC segments included in the export.",
    type: "npmrds_tmc_identification_imp",
    display_name: "NPMRDS TMC Identification Import Table",
    source_dependencies_names: [NpmrdsDataSources.NpmrdsTravelTimesExportEtl],
  },

  [NpmrdsDataSources.NpmrdsTmcIdentification]: {
    description: "NPMRDS TMC Identification Authoritative Data Source.",
    type: "npmrds_tmc_identification",
    display_name: "NPMRDS TMC Identification",
    source_dependencies_names: [NpmrdsDataSources.NpmrdsTmcIdentificationImp],
  },
};

const g = new Graph({
  directed: true,
  multigraph: false,
  compound: false,
});

for (const [name, meta] of Object.entries(
  npmrdsDataSourcesInitialMetadataByName
)) {
  const { source_dependencies_names } = meta;
  if (!source_dependencies_names) {
    g.setNode(name);
    continue;
  }

  // We flatten the depencencies because NpmrdsTravelTimes's is 2-dimensional.
  for (const dependencyName of _.flattenDeep(source_dependencies_names) || []) {
    if (name !== dependencyName) {
      g.setEdge(`${dependencyName}`, `${name}`);
    }
  }
}

export const toposortedSourceNames = GraphAlgorithms.topsort(g);

export const toposortedNpmrdsDataSourcesInitialMetadata =
  toposortedSourceNames.map((name) => ({
    name,
    ...npmrdsDataSourcesInitialMetadataByName[name],
  }));
