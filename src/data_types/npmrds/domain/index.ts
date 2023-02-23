/* eslint-disable max-len */

import { Graph, alg as GraphAlgorithms } from "graphlib";
import _ from "lodash";

export enum NpmrdsDataSources {
  NpmrdsTravelTimesExportRitis = "NpmrdsTravelTimesExportRitis",

  NpmrdsAllVehTravelTimesExport = "NpmrdsAllVehTravelTimesExport",
  NpmrdsPassVehTravelTimesExport = "NpmrdsPassVehTravelTimesExport",
  NpmrdsFrgtTrkTravelTimesExport = "NpmrdsFrgtTrkTravelTimesExport",

  NpmrdsTravelTimesExportSqlite = "NpmrdsTravelTimesExportSqlite",

  NpmrdsTravelTimesCsv = "NpmrdsTravelTimesCsv",
  NpmrdsTravelTimesImp = "NpmrdsTravelTimesImp",
  NpmrdsTravelTimes = "NpmrdsTravelTimes",

  NpmrdsTmcIdentificationCsv = "NpmrdsTmcIdentificationCsv",
  NpmrdsTmcIdentificationImp = "NpmrdsTmcIdentificationImp",
  NpmrdsTmcIdentification = "NpmrdsTmcIdentification",
}

export enum NpmrdsDatabaseSchemas {
  NpmrdsTravelTimesImp = "npmrds_travel_times_imports",

  NpmrdsTravelTimes = "npmrds_travel_times",

  NpmrdsTmcIdentificationImp = "npmrds_tmc_identification_imp",
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
  },

  [NpmrdsDataSources.NpmrdsAllVehTravelTimesExport]: {
    description:
      "Raw RITIS NPMRDS all vehicles travel times export ZIP archive downloaded as part of a RITIS NPMRDS Travel Times Export. The ZIP archive includes the all vehicle travel times CSV and a TMC_Identification CSV that provides road segment metadata.",
    type: "npmrds_data_source_travel_times_export",
    display_name: "Raw NPMRDS All Vehicles Travel Times Export",
    source_dependencies_names: [NpmrdsDataSources.NpmrdsTravelTimesExportRitis],
  },

  [NpmrdsDataSources.NpmrdsPassVehTravelTimesExport]: {
    description:
      "Raw RITIS NPMRDS passenger vehicles travel times export ZIP archive downloaded as part of a RITIS NPMRDS Travel Times Export. The ZIP archive includes the passenger vehicle travel times CSV and a TMC_Identification CSV that provides road segment metadata.",
    type: "npmrds_data_source_travel_times_export",
    display_name: "Raw NPMRDS Passenger Vehicles Travel Times Export",
    source_dependencies_names: [NpmrdsDataSources.NpmrdsTravelTimesExportRitis],
  },

  [NpmrdsDataSources.NpmrdsFrgtTrkTravelTimesExport]: {
    description:
      "Raw RITIS NPMRDS freight truck travel times export ZIP archive downloaded as part of a RITIS NPMRDS Travel Times Export. The ZIP archive includes the freight trucks travel times CSV and a TMC_Identification CSV that provides road segment metadata.",
    type: "npmrds_data_source_travel_times_export",
    display_name: "Raw NPMRDS Freight Truck Travel Times Export",
    source_dependencies_names: [NpmrdsDataSources.NpmrdsTravelTimesExportRitis],
  },

  [NpmrdsDataSources.NpmrdsTravelTimesCsv]: {
    description:
      "NPMRDS Travel Times CSV that joins the all vehicle, passenger vehicle, and freight truck travel times CSVs on (TMC, date, epoch).",
    type: "npmrds_travel_times_csv",
    display_name: "NPMRDS Travel Times CSV",
    source_dependencies_names: [
      NpmrdsDataSources.NpmrdsAllVehTravelTimesExport,
      NpmrdsDataSources.NpmrdsPassVehTravelTimesExport,
      NpmrdsDataSources.NpmrdsFrgtTrkTravelTimesExport,
    ],
  },

  [NpmrdsDataSources.NpmrdsTravelTimesExportSqlite]: {
    description:
      'NPMRDS Travel Times SQLite DB that contains two tables. The "npmrds" table joins the NPMRDS Travel Times Export all vehicle, passenger vehicle, and freight truck travel times CSVs. The "tmc_idenification" table contains the TMC identification CSV included in the export. This file is an intermediary product of the ETL process and is preserved for analysis purposes.',
    type: "npmrds_travel_times_export_sqlite",
    display_name: "NPMRDS Travel Times Export SQLite",
    source_dependencies_names: [
      NpmrdsDataSources.NpmrdsAllVehTravelTimesExport,
      NpmrdsDataSources.NpmrdsPassVehTravelTimesExport,
      NpmrdsDataSources.NpmrdsFrgtTrkTravelTimesExport,
      NpmrdsDataSources.NpmrdsTmcIdentificationCsv,
    ],
  },

  [NpmrdsDataSources.NpmrdsTravelTimesImp]: {
    description:
      "Database table containing the NPMRDS Travel Times imported into the database from an NpmrdsTravelTimesExportRitis. These imports are non-authoritative until made authoritative after QA. Authoritative versions are integrated into the NpmrdsTravelTimes data type.",
    type: "npmrds_travel_times_imp",
    display_name: "NPMRDS Travel Times Import",
    source_dependencies_names: [
      NpmrdsDataSources.NpmrdsTravelTimesExportSqlite,
    ],
  },

  [NpmrdsDataSources.NpmrdsTravelTimes]: {
    description:
      "Database table containing the authoritative NPMRDS Travel Times. The NPMRDS Authoritative Travel Times Database Table combines many NPMRDS Travel Times Imports.",
    type: "npmrds_travel_times",
    display_name: "NPMRDS Travel Times",
    source_dependencies_names: [NpmrdsDataSources.NpmrdsTravelTimesImp],
  },

  [NpmrdsDataSources.NpmrdsTmcIdentificationCsv]: {
    description:
      "Raw NPMRDS TMC Identification CSV included in the Raw NPMRDS All Vehicles Travel Times Export. This CSV contains metadata describing the TMC segments included in the export.",
    type: "npmrds_tmc_identification_csv",
    display_name: "NPMRDS TMC Identification CSV",
    source_dependencies_names: [
      NpmrdsDataSources.NpmrdsAllVehTravelTimesExport,
    ],
  },

  [NpmrdsDataSources.NpmrdsTmcIdentificationImp]: {
    description:
      "Database table containing the imported raw NPMRDS TMC Identification CSV. This table contains metadata describing the TMC segments included in the export.",
    type: "npmrds_tmc_identification_imp",
    display_name: "NPMRDS TMC Identification Import Table",
    source_dependencies_names: [
      NpmrdsDataSources.NpmrdsTravelTimesExportSqlite,
    ],
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
