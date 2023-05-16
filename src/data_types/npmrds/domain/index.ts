/* eslint-disable max-len */

import { Graph, alg as GraphAlgorithms } from "graphlib";
import _ from "lodash";

import { PgEnv } from "data_manager/dama_db/postgres/PostgreSQL";
import { EtlContextId } from "data_manager/contexts";
import { DataSourceInitialMetadata } from "data_manager/meta/domain";

export type NpmrdsTmc = string;

export type NpmrdsDataYear = number;
export type NpmrdsDataMonth = 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12;
export type NpmrdsDataDayOfMonth =
  | 1
  | 2
  | 3
  | 4
  | 5
  | 6
  | 7
  | 8
  | 9
  | 10
  | 11
  | 12
  | 13
  | 14
  | 15
  | 16
  | 17
  | 18
  | 19
  | 20
  | 21
  | 22
  | 23
  | 24
  | 25
  | 26
  | 27
  | 28
  | 29
  | 30
  | 31;

export type NpmrdsDownloadName = string;

export type EtlEvent = {
  type: string;
  payload?: any;
  meta?: any;
  error?: boolean;
};

export enum NpmrdsState {
  ak = "ak",
  al = "al",
  ar = "ar",
  az = "az",
  ca = "ca",
  co = "co",
  ct = "ct",
  dc = "dc",
  de = "de",
  fl = "fl",
  ga = "ga",
  hi = "hi",
  ia = "ia",
  id = "id",
  il = "il",
  in = "in",
  ks = "ks",
  ky = "ky",
  la = "la",
  ma = "ma",
  md = "md",
  me = "me",
  mi = "mi",
  mn = "mn",
  mo = "mo",
  ms = "ms",
  mt = "mt",
  nc = "nc",
  nd = "nd",
  ne = "ne",
  nh = "nh",
  nj = "nj",
  nm = "nm",
  nv = "nv",
  ny = "ny",
  oh = "oh",
  ok = "ok",
  or = "or",
  pa = "pa",
  pr = "pr",
  ri = "ri",
  sc = "sc",
  sd = "sd",
  tn = "tn",
  tx = "tx",
  ut = "ut",
  va = "va",
  vt = "vt",
  wa = "wa",
  wi = "wi",
  wv = "wv",
  wy = "wy",

  ab = "ab",
  bc = "bc",
  mb = "mb",
  nb = "nb",
  on = "on",
  qc = "qc",
  sk = "sk",
}

export type DataDate = {
  year: number;
  month: number;
  day: number;

  iso: string;
  iso_date: string;
};

export type DataDateRange = {
  start: DataDate & {
    is_start_of_month: boolean;
    is_start_of_week: boolean;
  };

  end: DataDate & {
    is_end_of_month: boolean;
    is_end_of_week: boolean;
  };
};

export enum NpmrdsDataSources {
  // The Raw RITIS NPMRDS Travel Times Export download with its three ZIP archives.
  NpmrdsTravelTimesExportRitis = "NpmrdsTravelTimesExportRitis",

  // The ETL output
  NpmrdsTravelTimesExportEtl = "NpmrdsTravelTimesExportEtl",

  // The NPMRDS Travel Times Postgres DB Tables
  NpmrdsTravelTimesImports = "NpmrdsTravelTimesImports",
  NpmrdsTravelTimes = "NpmrdsTravelTimes",

  // The NPMRDS TMC Identification Postgres DB Tables
  NpmrdsTmcIdentificationImports = "NpmrdsTmcIdentificationImports",
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
  NpmrdsTravelTimesImports = "npmrds_travel_times_imports",

  NpmrdsTravelTimes = "npmrds_travel_times",

  NpmrdsTmcIdentificationImports = "npmrds_tmc_identification_imports",
}

// NOTE:  These the DataSourceMeta property values could become stale if they are later updated.
//        Therefore, these values are for initialization ONLY.
//        Current values should be queried from the DB.
export const npmrdsDataSourcesInitialMetadata: DataSourceInitialMetadata[] = [
  {
    name: NpmrdsDataSources.NpmrdsTravelTimesExportRitis,
    description:
      "Raw RITIS NPMRDS Travel Times Export ZIP archives as downloaded from RITIS. Comprised of the all vehicle, passenger vehicle, and freight truck exports.",
    type: "npmrds_travel_times_export_ritis",
    display_name: "NPMRDS Travel Times Export (RITIS)",
    source_dependencies_names: null,
    metadata: {
      elements: {
        [NpmrdsTravelTimesExportRitisElements.NpmrdsAllVehiclesTravelTimesExport]:
          {
            name: NpmrdsTravelTimesExportRitisElements.NpmrdsAllVehiclesTravelTimesExport,
            description:
              "Raw RITIS NPMRDS all vehicles travel times export ZIP archive downloaded as part of a RITIS NPMRDS Travel Times Export. The ZIP archive includes the all vehicle travel times CSV and a TMC_Identification CSV that provides road segment metadata.",
            type: "npmrds_data_source_travel_times_export",
            display_name: "Raw NPMRDS All Vehicles Travel Times Export",
          },

        [NpmrdsTravelTimesExportRitisElements.NpmrdsPassengerVehiclesTravelTimesExport]:
          {
            name: NpmrdsTravelTimesExportRitisElements.NpmrdsPassengerVehiclesTravelTimesExport,
            description:
              "Raw RITIS NPMRDS passenger vehicles travel times export ZIP archive downloaded as part of a RITIS NPMRDS Travel Times Export. The ZIP archive includes the passenger vehicle travel times CSV and a TMC_Identification CSV that provides road segment metadata.",
            type: "npmrds_data_source_travel_times_export",
            display_name: "Raw NPMRDS Passenger Vehicles Travel Times Export",
          },

        [NpmrdsTravelTimesExportRitisElements.NpmrdsFreightTrucksTravelTimesExport]:
          {
            name: NpmrdsTravelTimesExportRitisElements.NpmrdsFreightTrucksTravelTimesExport,
            description:
              "Raw RITIS NPMRDS freight truck travel times export ZIP archive downloaded as part of a RITIS NPMRDS Travel Times Export. The ZIP archive includes the freight trucks travel times CSV and a TMC_Identification CSV that provides road segment metadata.",
            type: "npmrds_data_source_travel_times_export",
            display_name: "Raw NPMRDS Freight Truck Travel Times Export",
          },
      },
    },
  },

  {
    name: NpmrdsDataSources.NpmrdsTravelTimesExportEtl,
    description:
      "Output of the ETL processing performed on the NpmrdsTravelTimesExportRitis data source.",
    type: "npmrds_travel_times_export_etl",
    display_name: "NPMRDS Travel Times Export (ETL Output)",
    source_dependencies_names: [NpmrdsDataSources.NpmrdsTravelTimesExportRitis],
    metadata: {
      elements: {
        [NpmrdsTravelTimesExportEtlElements.NpmrdsTravelTimesExportSqlite]: {
          name: NpmrdsTravelTimesExportEtlElements.NpmrdsTravelTimesExportSqlite,
          description:
            'NPMRDS Travel Times SQLite DB that contains two tables. The "npmrds" table joins the NPMRDS Travel Times Export all vehicle, passenger vehicle, and freight truck travel times CSVs. The "tmc_idenification" table contains the TMC identification CSV included in the export. This file is an intermediary product of the ETL process and is preserved for analysis purposes.',
          type: "npmrds_travel_times_export_sqlite",
          display_name: "NPMRDS Travel Times Export SQLite",
        },

        [NpmrdsTravelTimesExportEtlElements.NpmrdsTmcIdentificationCsv]: {
          name: NpmrdsTravelTimesExportEtlElements.NpmrdsTmcIdentificationCsv,
          description:
            "Raw NPMRDS TMC Identification CSV included in the Raw NPMRDS All Vehicles Travel Times Export. This CSV contains metadata describing the TMC segments included in the export.",
          type: "npmrds_tmc_identification_csv",
          display_name: "NPMRDS TMC Identification CSV",
          source_dependencies_names: [
            NpmrdsDataSources.NpmrdsTravelTimesExportRitis,
          ],
        },

        [NpmrdsTravelTimesExportEtlElements.NpmrdsTravelTimesCsv]: {
          name: NpmrdsTravelTimesExportEtlElements.NpmrdsTravelTimesCsv,
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

  {
    name: NpmrdsDataSources.NpmrdsTravelTimesImports,
    description:
      "Database table containing the NPMRDS Travel Times imported into the database from an NpmrdsTravelTimesExportRitis. These imports are non-authoritative until made authoritative after QA. Authoritative versions are integrated into the NpmrdsTravelTimes data type.",
    type: "npmrds_travel_times_imp",
    display_name: "NPMRDS Travel Times Import",
    source_dependencies_names: [NpmrdsDataSources.NpmrdsTravelTimesExportEtl],
  },

  {
    name: NpmrdsDataSources.NpmrdsTravelTimes,
    description:
      "Database table containing the authoritative NPMRDS Travel Times. The NPMRDS Authoritative Travel Times Database Table combines many NPMRDS Travel Times Imports.",
    type: "npmrds_travel_times",
    display_name: "NPMRDS Travel Times",
    source_dependencies_names: [NpmrdsDataSources.NpmrdsTravelTimesImports],
  },

  {
    name: NpmrdsDataSources.NpmrdsTmcIdentificationImports,
    description:
      "Database table containing the imported raw NPMRDS TMC Identification CSV. This table contains metadata describing the TMC segments included in the export.",
    type: "npmrds_tmc_identification_imp",
    display_name: "NPMRDS TMC Identification Import Table",
    source_dependencies_names: [NpmrdsDataSources.NpmrdsTravelTimesExportEtl],
  },

  {
    name: NpmrdsDataSources.NpmrdsTmcIdentification,
    description: "NPMRDS TMC Identification Authoritative Data Source.",
    type: "npmrds_tmc_identification",
    display_name: "NPMRDS TMC Identification",
    source_dependencies_names: [
      NpmrdsDataSources.NpmrdsTmcIdentificationImports,
    ],
  },
];

export const npmrdsDataSourcesInitialMetadataByName = <
  Record<NpmrdsDataSources, DataSourceInitialMetadata>
>npmrdsDataSourcesInitialMetadata.reduce((acc, meta) => {
  const { name } = meta;

  acc[name] = meta;

  return acc;
}, {});

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

export const toposortedSourceNames: string[] = GraphAlgorithms.topsort(g);

export const toposortedNpmrdsDataSourcesInitialMetadata: DataSourceInitialMetadata[] =
  toposortedSourceNames.map((name) => ({
    name,
    ...npmrdsDataSourcesInitialMetadataByName[name],
  }));

export type NpmrdsExportRequest = {
  state: NpmrdsState;
  start_date: string;
  end_date: string;
  is_expanded: boolean;
};

export type NpmrdsExportMetadata = NpmrdsExportRequest & {
  name: string;
  year: NpmrdsDataYear;
  is_complete_month: boolean;
  is_complete_week: boolean;
  pg_env: PgEnv;
  etl_context_id: EtlContextId;
  parent_context_id: EtlContextId | null;
};

export type NpmrdsExportTransformOutput = {
  npmrdsDownloadName: string;

  npmrdsTravelTimesExportRitis: string;

  npmrdsAllVehiclesTravelTimesExport: string;
  npmrdsPassengerVehiclesTravelTimesExport: string;
  npmrdsFreightTrucksTravelTimesExport: string;

  npmrdsTravelTimesSqliteDb: string;

  npmrdsTravelTimesExportSqlite: string;
  npmrdsTmcIdentificationCsv: string;
  npmrdsTravelTimesCsv: string;
};

export enum TaskQueue {
  AGGREGATE_ETL = "npmrds:AGGREGATE_ETL",
  DOWNLOAD_EXPORT = "npmrds:DOWNLOAD_EXPORT",
  TRANSFORM_EXPORT = "npmrds:TRANSFORM_EXPORT",
  GENERAL_WORKER = "npmrds:GENERAL_WORKER",
}
