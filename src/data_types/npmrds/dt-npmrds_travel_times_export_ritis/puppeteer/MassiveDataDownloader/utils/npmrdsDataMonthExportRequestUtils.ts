import { inspect } from "util";

import _ from "lodash";
import { DateTime } from "luxon";
import { v4 as uuid } from "uuid";

import logger from "data_manager/logger";

import PageNetworkUtils from "../../PageNetworkUtils";

import { DataDateRange } from "data_types/npmrds/domain";

import { createDataDateRange } from "data_types/npmrds/utils/dates";

import {
  NpmrdsDownloadName,
  RitisExportNpmrdsDataSource,
  ParsedHttpRequest,
  ParsedHttpResponse,
  NpmrdsDownloadRequest,
} from "../../../domain";

import {
  NpmrdsTravelTimeUnits,
  MassiveDataDownloaderDataMeasure,
  MassiveDataDownloaderExportRequest,
  MassiveDataDownloaderExportResponse,
  MassiveDataDownloaderExportResponses,
} from "../../../domain";

import * as tmcsList from "../../../utils/tmcsList";

const allNpmrdsDataSources = [
  RitisExportNpmrdsDataSource.ALL_VEHICLES,
  RitisExportNpmrdsDataSource.PASSENGER_VEHICLES,
  RitisExportNpmrdsDataSource.TRUCKS,
];

// It can take quite a while for RITIS to start the exports for larger states.
// const TWO_HOURS = 1000 * 60 * 60 * 2; // milliseconds->seconds->minutes->hours
const THIRTY_MINUTES = 1000 * 60 * 30; // milliseconds->seconds->minutes->hours

const EXPORT_REQUEST_URL = "https://npmrds.ritis.org/export/submit/";

export function parseExportRequest(data: MassiveDataDownloaderExportRequest): {
  name: NpmrdsDownloadName;
  dataSource: RitisExportNpmrdsDataSource;
} {
  const {
    request_url,
    request_body: { NAME: name, DATASOURCES: requestDataSources },
  } = data;

  if (request_url !== EXPORT_REQUEST_URL) {
    throw new Error(
      `INVARIANT VIOLATION: Unexpected response_url ${request_url}`
    );
  }

  if (requestDataSources.length !== 1) {
    throw new Error(
      `INVARIANT VIOLATION: datasources array length ${requestDataSources.length} !== 1`
    );
  }

  const [{ id: datasourceId }] = requestDataSources;

  let dataSource: RitisExportNpmrdsDataSource;

  if (/combined$/i.test(datasourceId)) {
    dataSource = RitisExportNpmrdsDataSource.ALL_VEHICLES;
  } else if (/passenger$/i.test(datasourceId)) {
    dataSource = RitisExportNpmrdsDataSource.PASSENGER_VEHICLES;
  } else if (/truck$/i.test(datasourceId)) {
    dataSource = RitisExportNpmrdsDataSource.TRUCKS;
  } else {
    throw new Error(
      `INVARIANT VIOLATION: could not parse datasource id ${datasourceId}`
    );
  }

  return {
    name,
    dataSource,
  };
}

export function parseMassiveDataDownloaderExportResponse(
  mddExportResp: MassiveDataDownloaderExportResponse
): {
  name: NpmrdsDownloadName;
  dataSource: RitisExportNpmrdsDataSource;
} {
  const {
    response_url,
    response_body: {
      arguments: { datasources: responseDataSources },
      name,
    },
  } = mddExportResp;

  if (response_url !== EXPORT_REQUEST_URL) {
    throw new Error(
      `INVARIANT VIOLATION: Unexpected response_url ${response_url}`
    );
  }

  if (responseDataSources.length !== 1) {
    throw new Error(
      `INVARIANT VIOLATION: datasources array length ${responseDataSources.length} !== 1`
    );
  }

  const [{ id: datasourceId }] = responseDataSources;

  let dataSource: RitisExportNpmrdsDataSource;

  if (/combined$/i.test(datasourceId)) {
    dataSource = RitisExportNpmrdsDataSource.ALL_VEHICLES;
  } else if (/passenger$/i.test(datasourceId)) {
    dataSource = RitisExportNpmrdsDataSource.PASSENGER_VEHICLES;
  } else if (/truck$/i.test(datasourceId)) {
    dataSource = RitisExportNpmrdsDataSource.TRUCKS;
  } else {
    throw new Error(
      `INVARIANT VIOLATION: could not parse datasource id ${datasourceId}`
    );
  }

  return {
    name,
    dataSource,
  };
}

export function getNpmrdsExportRequestDateRange(
  mddExSubReq: MassiveDataDownloaderExportRequest
): DataDateRange {
  const {
    request_url,
    request_body: { DATERANGES: dateRanges },
  } = mddExSubReq;

  if (request_url !== EXPORT_REQUEST_URL) {
    throw new Error(
      `INVARIANT VIOLATION: Unexpected response_url ${request_url}`
    );
  }

  dateRanges
    .sort((a: { start_date: string }, b: { start_date: string }) =>
      a.start_date.localeCompare(b.start_date)
    )
    .map(({ start_date: s, end_date: e }) => ({
      start_date: s.trim(),
      end_date: e.trim(),
    }));

  // NOTE: luxon refers to this format as SQL format
  const expected_timestamp_format_re = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/;

  const all_dates_are_in_expected_format = dateRanges.every(
    ({ start_date, end_date }) =>
      expected_timestamp_format_re.test(start_date) &&
      expected_timestamp_format_re.test(end_date)
  );

  if (!all_dates_are_in_expected_format) {
    throw new Error(
      "It appears RITIS changed the timestamp formats in the DATERANGES list."
    );
  }

  logger.silly(
    `npmrdsDataMonthExportRequestUtils getNpmrdsExportRequestDateRange ${JSON.stringify(
      dateRanges,
      null,
      4
    )}`
  );

  const { start_date: begin_timestamp } = dateRanges[0];
  const { end_date: end_timestamp } = dateRanges[dateRanges.length - 1];

  const start_date_time = DateTime.fromSQL(begin_timestamp);
  const end_date_time = DateTime.fromSQL(end_timestamp).minus({ second: 1 });

  logger.debug(
    `npmrdsDataMonthExportRequestUtils getNpmrdsExportRequestDateRange:
       start_date_time=${start_date_time}
       end_date_time=${end_date_time}
     \n=========================================================================
    `
  );

  if (start_date_time.year !== end_date_time.year) {
    throw new Error(
      "INVARIANT VIOLATION: Requested NPMRDS Export Date Ranges MUST be within a calendar year."
    );
  }

  const midnight_timestamp_re = / 00:00:00$/;

  let prev_end_date_time: DateTime | null = null;

  for (const { start_date, end_date } of dateRanges) {
    if (
      !(
        midnight_timestamp_re.test(start_date) &&
        midnight_timestamp_re.test(end_date)
      )
    ) {
      throw new Error(
        `Date range elements not from midnight to midnight: ${start_date} to ${end_date}`
      );
    }

    const s_date_time = DateTime.fromSQL(start_date);
    const e_date_time = DateTime.fromSQL(end_date); // Exclusive. Start of following day.

    if (prev_end_date_time !== null) {
      if (s_date_time.toMillis() !== prev_end_date_time.toMillis()) {
        throw new Error(
          `Gap in the date range: ${prev_end_date_time.toISO()} to ${s_date_time.toISO()}`
        );
      }
    }

    prev_end_date_time = e_date_time;

    const incremented_s_date_time = s_date_time.plus({ day: 1 });

    if (incremented_s_date_time.toMillis() !== e_date_time.toMillis()) {
      logger.debug(
        `Throwing "INVARIANT BROKEN: The DATERANGES entries are not 24 hour periods." because ${JSON.stringify(
          { s_date_time, e_date_time, incremented_s_date_time },
          null,
          4
        )}`
      );

      throw new Error(
        "INVARIANT BROKEN: The DATERANGES entries are not 24 hour periods."
      );
    }
  }

  const data_date_range = createDataDateRange(
    start_date_time.toISODate(),
    end_date_time.toISODate()
  );

  return data_date_range;
}

export function validateNpmrdsExportRequestDateRange(
  mddExSubReq: MassiveDataDownloaderExportRequest,
  npmrdsDataReq: NpmrdsDownloadRequest
) {
  const { date_range: etl_task_req_date_range } = npmrdsDataReq;

  const http_req_date_range = getNpmrdsExportRequestDateRange(mddExSubReq);

  if (!_.isEqual(http_req_date_range, etl_task_req_date_range)) {
    throw new Error(
      `Incorrect NPMRDS export date range: ${JSON.stringify(
        {
          etl_task_req_date_range,
          http_req_date_range,
        },
        null,
        4
      )}).`
    );
  }
}

export async function confirmAllTmcsInExportRequest(
  mddExSubReq: MassiveDataDownloaderExportRequest
) {
  const expectedA = await tmcsList.getTmcs();

  const actualTmcs = mddExSubReq.request_body.TMCS;

  const actualS = new Set(actualTmcs);

  const omitted = expectedA.filter((tmc: string) => !actualS.has(tmc));

  if (omitted.length > 0) {
    throw new Error(
      `The following TMCs are missing from the Export request: ${JSON.stringify(
        omitted
      )}`
    );
  }
}

export function confirmAllDataMeasuresInExportRequest(
  mddExSubReq: MassiveDataDownloaderExportRequest
) {
  const dataSourceDescriptors = mddExSubReq.request_body.DATASOURCES;

  if (!dataSourceDescriptors || dataSourceDescriptors.length !== 1) {
    console.error(JSON.stringify(mddExSubReq.request_body, null, 4));
    throw new Error("INVARIANT BROKEN: dataSourceDescriptors.length !== 1");
  }

  const [{ columns }] = dataSourceDescriptors;

  const expectedA = Object.values(MassiveDataDownloaderDataMeasure);

  const actualS = new Set(columns);

  const omitted = expectedA.filter((m) => !actualS.has(m));

  if (omitted.length > 0) {
    throw new Error(
      `The following data source measures are missing from the Export request: ${JSON.stringify(
        omitted
      )}`
    );
  }
}

export function confirmTimeUnitsIsSecondsInExportRequest(
  mddExSubReq: MassiveDataDownloaderExportRequest
) {
  const { TRAVELTIMEUNITS } = mddExSubReq.request_body;

  if (
    TRAVELTIMEUNITS.toLowerCase() !==
    NpmrdsTravelTimeUnits.seconds.toLowerCase()
  ) {
    throw new Error("TravelTime units !== seconds: ${TRAVELTIMEUNITS}");
  }
}

export function confirmNullsIncludedInExportRequest(
  mddExSubReq: MassiveDataDownloaderExportRequest
) {
  const { ADDNULLRECORDS } = mddExSubReq.request_body;

  if (ADDNULLRECORDS !== true) {
    throw new Error("NULL records not included.");
  }
}

export function confirmNoTravelTimeAveragingInExportRequest(
  mddExSubReq: MassiveDataDownloaderExportRequest
) {
  const { AVERAGINGWINDOWSIZE } = mddExSubReq.request_body;

  if (+AVERAGINGWINDOWSIZE !== 0) {
    throw new Error("Export request averaging is on.");
  }
}

export function confirmDownloadNameInExportRequest(
  mddExSubReq: MassiveDataDownloaderExportRequest,
  npmrdsDataReq: NpmrdsDownloadRequest
) {
  const { NAME } = mddExSubReq.request_body;

  if (NAME !== npmrdsDataReq.name) {
    throw new Error(
      `Export request download name mismatch: "${NAME}" !== "${npmrdsDataReq.name}"`
    );
  }
}

export function confirmNoEmailNotificationInExportRequest(
  mddExSubReq: MassiveDataDownloaderExportRequest
) {
  const { SENDNOTIFICATIONEMAIL } = mddExSubReq.request_body;

  if (SENDNOTIFICATIONEMAIL !== false) {
    throw new Error("Email notifications not disabled.");
  }
}

export async function validateExportRequest(
  mddExSubReq: MassiveDataDownloaderExportRequest,
  npmrdsDataReq: NpmrdsDownloadRequest
) {
  const errorMessages: string[] = [];

  try {
    validateNpmrdsExportRequestDateRange(mddExSubReq, npmrdsDataReq);
  } catch (err) {
    errorMessages.push((<Error>err).message);
  }

  try {
    await confirmAllTmcsInExportRequest(mddExSubReq);
  } catch (err) {
    errorMessages.push((<Error>err).message);
  }

  try {
    confirmAllDataMeasuresInExportRequest(mddExSubReq);
  } catch (err) {
    errorMessages.push((<Error>err).message);
  }

  try {
    confirmTimeUnitsIsSecondsInExportRequest(mddExSubReq);
  } catch (err) {
    errorMessages.push((<Error>err).message);
  }

  try {
    confirmNullsIncludedInExportRequest(mddExSubReq);
  } catch (err) {
    errorMessages.push((<Error>err).message);
  }

  try {
    confirmNoTravelTimeAveragingInExportRequest(mddExSubReq);
  } catch (err) {
    errorMessages.push((<Error>err).message);
  }

  try {
    confirmDownloadNameInExportRequest(mddExSubReq, npmrdsDataReq);
  } catch (err) {
    errorMessages.push((<Error>err).message);
  }

  try {
    confirmNoEmailNotificationInExportRequest(mddExSubReq);
  } catch (err) {
    errorMessages.push((<Error>err).message);
  }

  if (errorMessages.length) {
    const message = errorMessages.reduce((acc, msg) => {
      acc = `${acc}\n\t* ${msg}`;
      return acc;
    }, "Complete NPMRDS Data Month Export Request validation failed for the following reasons:");

    return message;
  }

  return null;
}

export function catchAllExportRequests(
  pageNetworkUtils: PageNetworkUtils,
  npmrdsDownloadName: NpmrdsDownloadName
): {
  startCatchingExportRequests: () => Promise<
    Record<RitisExportNpmrdsDataSource, MassiveDataDownloaderExportRequest>
  >;
  teardown: () => Promise<void>;
} {
  let requestsInterceptor: (req: ParsedHttpRequest) => Promise<void>;

  const exportRequestsByDataSourcePromise: Promise<
    Record<RitisExportNpmrdsDataSource, MassiveDataDownloaderExportRequest>
  > = new Promise(async (resolve, reject) => {
    //  In case the url changes, we want to timeout rather than hang.
    //    Timeout cleared after seenAllDataSourceRequests.
    const allReqsTimeout = setTimeout(async () => {
      return reject(
        new Error(
          "Timeout waiting for all data source export/submit requests. Did the URL change?"
        )
      );
    }, THIRTY_MINUTES);

    const exportReqsByDataSource: Record<
      RitisExportNpmrdsDataSource,
      MassiveDataDownloaderExportRequest | null
    > = {
      [RitisExportNpmrdsDataSource.ALL_VEHICLES]: null,
      [RitisExportNpmrdsDataSource.PASSENGER_VEHICLES]: null,
      [RitisExportNpmrdsDataSource.TRUCKS]: null,
    };

    requestsInterceptor = async (req: ParsedHttpRequest) => {
      const tracing_id = uuid();

      logger.silly(
        `=========================================================================
           npmrdsDataMonthExportRequestUtils startCatchingExportRequests intercepted:
             tracing_id: ${tracing_id}
             request: \n${inspect(req, {
               depth: null,
               compact: false,
               sorted: true,
             })}

             request_event.isInterceptResolutionHandled: ${req.request_event.isInterceptResolutionHandled()}
         \n=========================================================================
        `
      );

      if (req.request_url === EXPORT_REQUEST_URL) {
        logger.silly(
          `npmrdsDataMonthExportRequestUtils startCatchingExportRequests inspecting:
                tracing_id: ${tracing_id}
                request_url: ${req.request_url} 
          `
        );

        req = <MassiveDataDownloaderExportRequest>req;

        let reqDataSource: RitisExportNpmrdsDataSource;

        try {
          // Because of URL, we assume MassiveDataDownloaderExportRequest.
          //   TODO: Add JSON Schema validation of the request event object.
          // @ts-ignore
          const parsedExportReq = parseExportRequest(req);

          reqDataSource = parsedExportReq.dataSource;

          if (parsedExportReq.name !== npmrdsDownloadName) {
            throw new Error(
              `Unexpected NPMRDS Download Name: expected=${npmrdsDownloadName}, actual=${parsedExportReq.name}`
            );
          }

          exportReqsByDataSource[reqDataSource] = <
            MassiveDataDownloaderExportRequest
          >req;

          logger.silly(
            `npmrdsDataMonthExportRequestUtils startCatchingExportRequests
                tracing_id: ${tracing_id}
                request_event.isInterceptResolutionHandled: ${req.request_event.isInterceptResolutionHandled()}
            `
          );
        } catch (err) {
          // This is a fatal error. All the following logic depends on knowing the dataSource.
          return reject(err);
        }

        const seenAllDataSourceRequests = allNpmrdsDataSources.every((ds) =>
          Boolean(exportReqsByDataSource[ds])
        );

        if (seenAllDataSourceRequests) {
          clearTimeout(allReqsTimeout);

          return resolve(
            <
              Record<
                RitisExportNpmrdsDataSource,
                MassiveDataDownloaderExportRequest
              >
            >exportReqsByDataSource
          );
        }
      } else {
        logger.silly(
          `npmrdsDataMonthExportRequestUtils startCatchingExportRequests continuing response:
             tracing_id:  ${tracing_id}
             request_url: ${req.request_url} 
          `
        );

        await pageNetworkUtils.continuePendingRequest(req);
      }
    };
  });

  return {
    startCatchingExportRequests: async () => {
      await pageNetworkUtils.addParsedRequestInterceptor(requestsInterceptor);
      return await exportRequestsByDataSourcePromise;
    },

    teardown: () =>
      pageNetworkUtils.removeRequestInterceptor(requestsInterceptor),
  };
}

export async function validateNpmrdsDownloadRequestNetworkObjects(
  pageNetworkUtils: PageNetworkUtils,
  npmrds_download_request: NpmrdsDownloadRequest
) {
  if (!npmrds_download_request.name) {
    throw new Error("INVARIANT BROKEN: no npmrds_download_request.name");
  }

  try {
    const { startCatchingExportRequests, teardown } = catchAllExportRequests(
      pageNetworkUtils,
      npmrds_download_request.name
    );

    const proceed = async () => {
      await teardown();
      await pageNetworkUtils.continueAllPendingRequests();
    };

    const exportRequestsByDataSource = await startCatchingExportRequests();

    const validationErrorsByDataSource: Record<
      RitisExportNpmrdsDataSource,
      string | null
    > = {
      [RitisExportNpmrdsDataSource.ALL_VEHICLES]: null,
      [RitisExportNpmrdsDataSource.PASSENGER_VEHICLES]: null,
      [RitisExportNpmrdsDataSource.TRUCKS]: null,
    };

    for (const dataSource of allNpmrdsDataSources) {
      const req = exportRequestsByDataSource[dataSource];

      validationErrorsByDataSource[dataSource] = await validateExportRequest(
        req,
        npmrds_download_request
      );
    }

    return {
      error: null,
      exportRequestsByDataSource,
      validationErrorsByDataSource,
      proceed,
    };
  } catch (error) {
    return {
      error,
      exportRequestsByDataSource: null,
      validationErrorsByDataSource: null,
      proceed: null,
    };
  }
}

export type NpmrdsExportResponseObjects = Record<
  RitisExportNpmrdsDataSource,
  MassiveDataDownloaderExportResponse
>;

export async function collectNpmrdsExportResponseObjects(
  pageNetworkUtils: PageNetworkUtils,
  npmrds_download_request: NpmrdsDownloadRequest
): Promise<MassiveDataDownloaderExportResponses> {
  const { name: downloadName } = npmrds_download_request;

  const exportResponsesByDataSourcePromise =
    new Promise<MassiveDataDownloaderExportResponses>((resolve, reject) => {
      const exportResponsesByDataSource: MassiveDataDownloaderExportResponses =
        {
          [RitisExportNpmrdsDataSource.ALL_VEHICLES]: null,
          [RitisExportNpmrdsDataSource.PASSENGER_VEHICLES]: null,
          [RitisExportNpmrdsDataSource.TRUCKS]: null,
        };

      const responsesInterceptor = async (res: ParsedHttpResponse) => {
        if (res.request_url === EXPORT_REQUEST_URL) {
          const exportRes = <MassiveDataDownloaderExportResponse>res;
          // Because of URL, we assume MassiveDataDownloaderExportRequest.
          //   TODO: Add JSON Schema validation of the request event object.
          const { name: expResDlName, dataSource: expResDlDataSrc } =
            parseMassiveDataDownloaderExportResponse(exportRes);

          logger.debug(
            `MassiveDataDownloader collectNpmrdsExportResponseObjects ${JSON.stringify(
              { expResDlName, expResDlDataSrc },
              null,
              4
            )}`
          );

          try {
            if (expResDlName !== downloadName) {
              const actual = expResDlName;
              const expected = downloadName;
              throw new Error(
                `MassiveDataDownloaderDAO download names don't match: expected=${expected}, actual=${actual}`
              );
            }

            exportResponsesByDataSource[expResDlDataSrc] = exportRes;
          } catch (err) {
            // This is a fatal error. All the following logic depends on knowing the dataSource.
            return reject(err);
          }

          const seenAllDataSourceResponses = allNpmrdsDataSources.every((ds) =>
            Boolean(exportResponsesByDataSource[ds])
          );

          if (seenAllDataSourceResponses) {
            clearTimeout(allResponsesTimeout);
            pageNetworkUtils.removeResponseInterceptor(responsesInterceptor);

            return resolve(exportResponsesByDataSource);
          }
        }
      };

      const allResponsesTimeout = setTimeout(async () => {
        pageNetworkUtils.removeResponseInterceptor(responsesInterceptor);

        return reject(
          new Error(
            "Timeout waiting for all data source export/submit requests. Did the URL change?"
          )
        );
      }, THIRTY_MINUTES);

      pageNetworkUtils.addParsedResponseInterceptor(responsesInterceptor);
    });

  return await exportResponsesByDataSourcePromise;
}

/*
// FIXME: This does not work. Not aborting the requests. No idea why.
export async function abortAllExportRequests(
  pageNetworkUtils: PageNetworkUtils
) {
  const fn = async (event: HTTPRequest) => {
    const url = event.url();
    if (url === EXPORT_REQUEST_URL) {
      console.log("ABORTING REQUEST:", url);
      if (event.isInterceptResolutionHandled()) {
        console.warn("EXPORT REQUEST ALREADY HANDLED. CANNOT ABORT.");
        return;
      }
      await event.abort("failed", 100);
    } else {
      if (event.isInterceptResolutionHandled()) {
        console.warn("EXPORT REQUEST ALREADY HANDLED. CANNOT CONTINUE.");
        return;
      }
      console.log("CONTINUING REQUEST", url);
      await event.continue({}, DEFAULT_INTERCEPT_RESOLUTION_PRIORITY);
    }
  };

  await pageNetworkUtils.addRequestInterceptor(fn);
}
*/
