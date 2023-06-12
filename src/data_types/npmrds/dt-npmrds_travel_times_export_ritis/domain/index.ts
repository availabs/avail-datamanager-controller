import * as turf from "@turf/turf";

import { DataDateRange } from "data_types/npmrds/domain";

// re-exporting for convenience
export type {
  NpmrdsExportTransformOutput,
  NpmrdsExportRequest,
  NpmrdsExportMetadata,
} from "../../domain";

import {
  NpmrdsTmc,
  NpmrdsDataYear,
  NpmrdsDownloadName,
  NpmrdsState,
} from "../../domain";

export enum RitisExportNpmrdsDataSource {
  ALL_VEHICLES = "ALL_VEHICLES",
  PASSENGER_VEHICLES = "PASSENGER_VEHICLES",
  TRUCKS = "TRUCKS",
}

export type ParsedHttpRequest = {
  request_url: string;
  request_method: string;
  request_body: any;
  request_event: any;
};

export type ParsedHttpResponse = {
  request_url: string;
  request_method: string;
  request_body: any;
  response_url: string;
  response_status: number;
  response_from_cache: boolean;
  response_body: any;
};

export type NpmrdsDownloadRequest = {
  readonly name: NpmrdsDownloadName;
  readonly year: NpmrdsDataYear;
  readonly state: NpmrdsState;
  readonly is_expanded: boolean;

  readonly date_range: DataDateRange;
};

export type NpmrdsDownloadUrls = Record<RitisExportNpmrdsDataSource, string>;

export type NpmrdsTmcGeoJsonFeature = turf.Feature<
  turf.LineString | turf.MultiLineString
> & { id: NpmrdsTmc };

// TODO: Move this up to src/dataSources/RITIS/domain/

// TODO: Use the below to generate JSON Schema for the requests and responses.
//       Monitor requests/response for schema changes.
//         Have JSON Schema for different levels: info, warn, error

export enum MassiveDataDownloaderDataSource {
  PASSENGER_VEHICLES = "npmrds2_passenger",
  ALL_VEHICLES = "npmrds2_combined",
  TRUCKS = "npmrds2_truck",
}

export enum MassiveDataDownloaderDataMeasure {
  speed = "speed",
  average_speed = "average_speed",
  reference_speed = "reference_speed",
  travel_time_minutes = "travel_time_minutes",
  data_density = "data_density",
}

export type MassiveDataDownloaderDataSourceDescriptor = {
  id: MassiveDataDownloaderDataSource;
  columns: MassiveDataDownloaderDataMeasure[];
};

export type MassiveDataDownloaderRoadDetails = {
  SEGMENT_IDS: NpmrdsTmc[];
  DATASOURCE_ID: MassiveDataDownloaderDataSource;
  ATLAS_VERSION_ID: number;
};

export type MassiveDataDownloaderTimestampRange = {
  start_date: string;
  end_date: string;
};

export type MassiveDataDownloaderDateRange = { start: string; end: string };

export type NpmrdsTravelTimeAveragingWindowSizeMinutes = 0 | 10 | 15 | 60;

export enum NpmrdsTravelTimeUnits {
  seconds = "seconds",
  minutes = "minutes",
}

// TODO: investigate the domain of this type
export type NpmrdsTimesOfDay = {
  start: "00:00:00.000";
};

export type RitisDownloadRequestUuid = string;

export type MassiveDataDownloaderExportRequest = ParsedHttpRequest & {
  request_url: "https://npmrds.ritis.org/export/submit/";
  request_method: "POST";
  request_body: {
    DATASOURCES: MassiveDataDownloaderDataSourceDescriptor[];
    ROADPROVIDER: MassiveDataDownloaderDataSource;
    TMCS: NpmrdsTmc[];
    ROAD_DETAILS: [MassiveDataDownloaderRoadDetails];
    DATERANGES: MassiveDataDownloaderTimestampRange[];
    ENTIREROAD: boolean;
    NAME: NpmrdsDownloadName;
    DESCRIPTION: string;
    MERGE_FILES: boolean;
    AVERAGINGWINDOWSIZE: NpmrdsTravelTimeAveragingWindowSizeMinutes;
    EMAILADDRESS: string;
    SENDNOTIFICATIONEMAIL: boolean;
    ADDNULLRECORDS: boolean;
    TRAVELTIMEUNITS: NpmrdsTravelTimeUnits;
    COUNTRYCODE: string;
  };
};

export type MassiveDataDownloaderExportResponse =
  MassiveDataDownloaderExportRequest &
    ParsedHttpResponse & {
      response_url: "https://npmrds.ritis.org/export/submit/";
      response_body: {
        invoked_tstamp: null;
        removed_tstamp: null;
        uuid: RitisDownloadRequestUuid;
        hadoop_job: number;
        id: number;
        request_id: RitisDownloadRequestUuid;
        display_shapefile_link: true;
        status: number;
        path_to_result_file: null;
        arguments: {
          times: NpmrdsTimesOfDay[];
          datasources: MassiveDataDownloaderDataSourceDescriptor[];
          tmcs: NpmrdsTmc[];
          add_null_records: boolean;
          date_ranges: MassiveDataDownloaderTimestampRange[];
          country_code: string;
          merge_files: boolean;
          version: string;
          travel_time_units: NpmrdsTravelTimeUnits;
          averaging_window_size: NpmrdsTravelTimeAveragingWindowSizeMinutes;
          road_provider: MassiveDataDownloaderDataSource;
          name: NpmrdsDownloadName;
          dates: MassiveDataDownloaderDateRange[];
          road_details: MassiveDataDownloaderRoadDetails[];
          entire_road: boolean;
        };
        description: string;
        expiration_tstamp: string | null;
        downloaded: boolean;
        completed_tstamp: null;
        name: NpmrdsDownloadName;
        favorite: boolean;
        send_email_on_complete: boolean;
        url: string;
        requested_tstamp: string;
        email_address: string;
      };
    };

export type MassiveDataDownloaderExportResponses = Record<
  RitisExportNpmrdsDataSource,
  MassiveDataDownloaderExportResponse | null
>;

export type RitisExportRequestNetworkObjects = {
  all_vehicles_request: MassiveDataDownloaderExportRequest["request_body"];
  all_vehicles_response: MassiveDataDownloaderExportResponse["request_body"];

  passenger_vehicles_request: MassiveDataDownloaderExportRequest["request_body"];
  passenger_vehicles_response: MassiveDataDownloaderExportResponse["request_body"];

  trucks_request: MassiveDataDownloaderExportRequest["request_body"];
  trucks_response: MassiveDataDownloaderExportResponse["request_body"];
};

export type RawRitisExportRequestStatusUpdate = {
  // "created_tstamp": "2022-03-02T22:17:37.206771+00:00",
  created_tstamp: string;
  hadoop_status: {
    state: "UNDEFINED" | "SUCCEEDED";
    progress: number;
  };
  file_size: null | number;
  uuid: string;
  description: string;
  downloaded: boolean;
  display_shapefile_link: boolean;
  tool: "Massive Data Downloader";
  favorite: boolean;
  status: number;
};

export type RitisNpmrdsExportRequestStatusUpdate = {
  name: string;
  created_tstamp: string;
  hadoop_status: "UNDEFINED" | "SUCCEEDED";
  uuid: string;
  downloaded: boolean;
  status: number;
  url: string;
};

// NOTE: NpmrdsDownloadName used as the key since MyHistoryDAO oblivious to NpmrdsDownloadId
//         * NpmrdsDownloadName is available in the RawRitisExportRequestStatusUpdate.
//         * using NpmrdsDownloadId would require a lookup in the NpmrdsDownloadManager DB.
export type RitisNpmrdsExportRequestStatus = Record<
  NpmrdsDownloadName,
  RitisNpmrdsExportRequestStatusUpdate[]
>;

export type NpmrdsExportReadyMeta = {
  name: NpmrdsDownloadName;
  urls: NpmrdsDownloadUrls;
};

export type NpmrdsExportDownloadPaths = {
  [RitisExportNpmrdsDataSource.ALL_VEHICLES]: string;
  [RitisExportNpmrdsDataSource.PASSENGER_VEHICLES]: string;
  [RitisExportNpmrdsDataSource.TRUCKS]: string;
};

export type NpmrdsExportDownloadMeta = {
  name: NpmrdsDownloadName;
  download_paths: NpmrdsExportDownloadPaths;
};
