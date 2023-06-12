import { join } from "path";

import { Page, HTTPResponse } from "puppeteer";

import dama_events from "data_manager/events";
import logger from "data_manager/logger";
import { verifyIsInTaskEtlContext } from "data_manager/contexts";

import { getSessionPage } from "../Session";

import PageNetworkUtils from "../PageNetworkUtils";

import { NpmrdsDownloadName } from "data_types/npmrds/domain";

import {
  RitisExportNpmrdsDataSource,
  NpmrdsDownloadUrls,
  ParsedHttpResponse,
  RitisExportRequestNetworkObjects,
  RawRitisExportRequestStatusUpdate,
  RitisNpmrdsExportRequestStatus,
  NpmrdsExportReadyMeta,
} from "../../domain";

import getEtlMetadataDir from "../../utils/getEtlMetadataDir";

const MY_HISTORY_RESPONSE_URL = "https://npmrds.ritis.org/api/user_history/";

const tableContainerElementPath =
  "body > main > div > div:nth-child(1) > div.HistoryList > div.TableContainer";

export const NpmrdsExportReadyForDownloadEventType =
  ":NPMRDS_EXPORT_READY_FOR_DOWNLOAD";

export function parseExportStatusUpdate(
  res: ParsedHttpResponse
): RitisNpmrdsExportRequestStatus | null {
  let updatesByName: RitisNpmrdsExportRequestStatus | null = null;

  if (res.response_url === MY_HISTORY_RESPONSE_URL) {
    const {
      response_body,
    }: { response_body: RawRitisExportRequestStatusUpdate[] } = res;

    updatesByName = response_body.reduce((acc, update) => {
      const {
        description: name,
        created_tstamp,
        hadoop_status: { state: hadoop_status },
        uuid,
        downloaded,
        status,
      } = update;

      const url = `https://npmrds.ritis.org/export/download/${uuid}?dl=1`;

      acc[name] = acc[name] || [];
      acc[name].push({
        name,
        created_tstamp,
        hadoop_status,
        uuid,
        downloaded,
        status,
        url,
      });

      return acc;
    }, {});
  }

  return updatesByName;
}

export default class MyHistory {
  private static readonly pageUrl =
    "https://npmrds.ritis.org/analytics/my-history/";

  private _page: Page | null;
  private pageNetworkUtils?: PageNetworkUtils;

  constructor() {
    this._page = null;
  }

  private async _getPage(): Promise<Page> {
    if (!this._page) {
      logger.debug("MyHistory starting _getPage()");

      this._page = await getSessionPage();
      this.pageNetworkUtils = new PageNetworkUtils(this._page);

      if (logger.level === "silly") {
        const etl_metadata_dir = getEtlMetadataDir();

        const requests_path = join(
          etl_metadata_dir,
          "my_history_network_requests.ndjson"
        );

        logger.debug(`MyHistory logging network requests to ${requests_path}`);

        this.pageNetworkUtils.logRequests(requests_path);

        const responses_path = join(
          etl_metadata_dir,
          "my_history_network_responses.ndjson"
        );

        logger.debug(
          `MyHistory logging network responses to ${responses_path}`
        );

        this.pageNetworkUtils.logResponses(responses_path);
      }

      await this._page.goto(MyHistory.pageUrl, {
        timeout: 0,
      });

      await this._page.waitForSelector(tableContainerElementPath, {
        timeout: 1000 * 60 * 5,
      });

      logger.debug("MyHistory finished _getPage()");
    }

    return this._page;
  }

  async _disconnectPage() {
    logger.debug("MyHistory _disconnectPage");
    if (this._page !== null) {
      await this._page?.close();

      this._page = null;
    }
  }

  // FIXME: use PageNetworkUtils.addParsedResponseInterceptor
  private async registerDownloadRequestStatusEventEmitter(
    listener: (update: RitisNpmrdsExportRequestStatus) => Promise<void>
  ) {
    const page = await this._getPage();

    let misses = 0;
    const fn = async (event: HTTPResponse) => {
      const res = await PageNetworkUtils.parseHttpResponseEvent(event);

      const parsedUpdate = parseExportStatusUpdate(res);

      if (parsedUpdate === null) {
        ++misses;

        if (misses % 10 === 0) {
          console.log(
            "consecutive MyHistoryDAO parsedUpdate is null =",
            misses
          );
        }

        return;
      }

      if (parsedUpdate !== null) {
        misses = 0;
        await listener(parsedUpdate);
      }
    };

    const start = () => page.on("response", fn);
    const stop = () => page.off("response", fn);

    return {
      start,
      stop,
    };
  }

  // TODO: Handle degenerate cases.
  async waitForExportRequestReadyToDownload(
    ritis_export_request_network_objects: RitisExportRequestNetworkObjects
  ): Promise<NpmrdsExportReadyMeta> {
    verifyIsInTaskEtlContext();

    const events = await dama_events.getAllEtlContextEvents();

    let downloads_ready_event = events.find(
      ({ type }) => type === NpmrdsExportReadyForDownloadEventType
    );

    if (downloads_ready_event) {
      return downloads_ready_event.payload;
    }

    const {
      all_vehicles_response,
      passenger_vehicles_response,
      trucks_response,
    } = ritis_export_request_network_objects;

    const { name: npmrds_download_name }: { name: NpmrdsDownloadName } =
      all_vehicles_response;

    logger.debug(
      `MyHistoryDAO waitForExportRequestReadyToDownload npmrds_download_name=${npmrds_download_name}`
    );

    const uuids_to_data_sources: Record<string, RitisExportNpmrdsDataSource> = {
      [all_vehicles_response.uuid]: RitisExportNpmrdsDataSource.ALL_VEHICLES,
      [passenger_vehicles_response.uuid]:
        RitisExportNpmrdsDataSource.PASSENGER_VEHICLES,
      [trucks_response.uuid]: RitisExportNpmrdsDataSource.TRUCKS,
    };

    logger.debug(
      `MyHistoryDAO waitForExportRequestReadyToDownload uuids_to_data_sources=${JSON.stringify(
        uuids_to_data_sources,
        null,
        4
      )}`
    );

    return new Promise(async (resolve, reject) => {
      const listener = async (update: RitisNpmrdsExportRequestStatus) => {
        const download_req_update = update[npmrds_download_name];

        logger.debug(
          `MyHistoryDAO waitForExportRequestReadyToDownload download_req_update=${JSON.stringify(
            download_req_update,
            null,
            4
          )}`
        );

        if (download_req_update) {
          const done = download_req_update.every(
            ({ hadoop_status }) => hadoop_status === "SUCCEEDED"
          );

          logger.debug(
            `MyHistoryDAO waitForExportRequestReadyToDownload NPMRDS Export done=${done}`
          );

          if (done) {
            const urls = <NpmrdsDownloadUrls>download_req_update.reduce(
              (acc, { uuid, url }) => {
                const data_source = uuids_to_data_sources[uuid];

                if (!data_source) {
                  reject(
                    new Error(
                      `INVARIANT BROKEN: inconsistent RITIS UUIDs ${JSON.stringify(
                        { update, uuids_to_data_sources },
                        null,
                        4
                      )}`
                    )
                  );
                }

                acc[data_source] = url;

                return acc;
              },
              {}
            );

            const payload = {
              name: npmrds_download_name,
              urls,
            };

            downloads_ready_event = {
              type: NpmrdsExportReadyForDownloadEventType,
              payload,
            };

            await dama_events.dispatch(downloads_ready_event);

            stop();

            this._disconnectPage();

            logger.debug(
              `MyHistoryDAO waitForExportRequestReadyToDownload DONE ${payload}`
            );

            resolve(payload);
          }
        }
      };

      const { start, stop } =
        await this.registerDownloadRequestStatusEventEmitter(listener);

      start();
    });
  }
}
