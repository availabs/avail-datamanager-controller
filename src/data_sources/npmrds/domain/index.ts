/* eslint-disable max-len */

import { Graph, alg as GraphAlgorithms } from "graphlib";
import _ from "lodash";

export enum NpmrdsDataSources {
  NpmrdsTravelTimesExport = "NpmrdsTravelTimesExport",
  NpmrdsAllVehTravelTimesExport = "NpmrdsAllVehTravelTimesExport",
  NpmrdsPassVehTravelTimesExport = "NpmrdsPassVehTravelTimesExport",
  NpmrdsFrgtTrkTravelTimesExport = "NpmrdsFrgtTrkTravelTimesExport",
  NpmrdsTmcIdentificationCsv = "NpmrdsTmcIdentificationCsv",
  NpmrdsTravelTimesCsv = "NpmrdsTravelTimesCsv",
  NpmrdsTravelTimesExportSqlite = "NpmrdsTravelTimesExportSqlite",
  NpmrdsTravelTimesExportDb = "NpmrdsTravelTimesExportDb",
  NpmrdsAuthoritativeTravelTimesDb = "NpmrdsAuthoritativeTravelTimesDb",
}

// NOTE:  These the DataSourceMeta property values could become stale
//          if they are later updated.
//        Therefore, these values are for initialization.
//            Current values should be queried from the DB.
export const npmrdsDataSourcesInitialMetadataByName = {
  [NpmrdsDataSources.NpmrdsTravelTimesExport]: {
    description:
      "Raw RITIS NPMRDS Travel Times Export ZIP archives as downloaded from RITIS. Comprised of the all vehicle, passenger vehicle, and freight truck exports.",
    type: "npmrds_travel_times_export",
    display_name: "NPMRDS Travel Times Export",
    source_dependencies_names: null,
  },

  [NpmrdsDataSources.NpmrdsAllVehTravelTimesExport]: {
    description:
      "Raw RITIS NPMRDS all vehicles travel times export ZIP archive downloaded as part of a RITIS NPMRDS Travel Times Export. The ZIP archive includes the all vehicle travel times CSV and a TMC_Identification CSV that provides road segment metadata.",
    type: "npmrds_data_source_travel_times_export",
    display_name: "Raw NPMRDS All Vehicles Travel Times Export",
    source_dependencies_names: [NpmrdsDataSources.NpmrdsTravelTimesExport],
  },

  [NpmrdsDataSources.NpmrdsPassVehTravelTimesExport]: {
    description:
      "Raw RITIS NPMRDS passenger vehicles travel times export ZIP archive downloaded as part of a RITIS NPMRDS Travel Times Export. The ZIP archive includes the passenger vehicle travel times CSV and a TMC_Identification CSV that provides road segment metadata.",
    type: "npmrds_data_source_travel_times_export",
    display_name: "Raw NPMRDS Passenger Vehicles Travel Times Export",
    source_dependencies_names: [NpmrdsDataSources.NpmrdsTravelTimesExport],
  },

  [NpmrdsDataSources.NpmrdsFrgtTrkTravelTimesExport]: {
    description:
      "Raw RITIS NPMRDS freight truck travel times export ZIP archive downloaded as part of a RITIS NPMRDS Travel Times Export. The ZIP archive includes the freight trucks travel times CSV and a TMC_Identification CSV that provides road segment metadata.",
    type: "npmrds_data_source_travel_times_export",
    display_name: "Raw NPMRDS Freight Truck Travel Times Export",
    source_dependencies_names: [NpmrdsDataSources.NpmrdsTravelTimesExport],
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

  [NpmrdsDataSources.NpmrdsTravelTimesExportDb]: {
    description:
      "Database table containing the NPMRDS Travel Times loaded from an NpmrdsTravelTimesExport. Authoritative versions are integrated into the NpmrdsAuthoritativeTravelTimesDb data type.",
    type: "npmrds_travel_times_export_db",
    display_name: "NPMRDS Travel Times Export Database Table",
    source_dependencies_names: [
      NpmrdsDataSources.NpmrdsTravelTimesExportSqlite,
    ],
  },

  [NpmrdsDataSources.NpmrdsAuthoritativeTravelTimesDb]: {
    description:
      "Database table containing the authoritative NPMRDS Travel Times. The NPMRDS Authoritative Travel Times Database Table combines many NPMRDS Travel Times Export Database Tables.",
    type: "npmrds_authoritative_travel_times_db",
    display_name: "NPMRDS Authoritative Travel Times Database Table",
    // NOTE:  The source_dependencies_names is a 2-dimensional array for NpmrdsAuthoritativeTravelTimesDb.
    //        This is because NpmrdsAuthoritativeTravelTimesDb data sources is a tree.
    //          * At the leaves, the source_dependencies are NpmrdsTravelTimesCsv
    //          * At the internal nodes, the source_dependencies are NpmrdsTravelTimesExportDb
    source_dependencies_names: [
      [NpmrdsDataSources.NpmrdsTravelTimesExportDb],
      [NpmrdsDataSources.NpmrdsAuthoritativeTravelTimesDb],
    ],
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

  // We flatten the depencencies because NpmrdsAuthoritativeTravelTimesDb's is 2-dimensional.
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
