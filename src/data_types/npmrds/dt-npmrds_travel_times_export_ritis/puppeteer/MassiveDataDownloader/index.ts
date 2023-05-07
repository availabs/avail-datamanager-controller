/* eslint-disable no-unused-expressions */

/*
  The pupose of this module is to make the Massive Data Download request process
    less time consuming and error prone.

  NOTE: This code is intentionally designed to be EXTREMELY BRITTLE.

        If any assumptions about the structure of the MassiveDataDownloader page become unsound,
          this module MUST FAIL LOUDLY before sending malformed export requests.

            **  The validity of the production database's npmrds tables,  **
            **  and all deriviative tables or reports,                    **
            **  depends on this module's correctness.                     **

            Therefore, we must be EXTREMELY RISK ADVERSE within the module.

  NOTE: I'm not sure if we need to run ElementHandle.dispose every time we get an element handle.
*/

import { writeFile as writeFileAsync } from "fs/promises";
import { gzip } from "zlib";
import { join } from "path";
import { inspect, promisify } from "util";

import _ from "lodash";
import * as turf from "@turf/turf";

import { Page, ElementHandle, HTTPResponse, EvaluateFuncWith } from "puppeteer";

import {
  getEtlContextId,
  getPgEnv,
  verifyIsInTaskEtlContext,
} from "data_manager/contexts";
import dama_events from "data_manager/events";
import logger from "data_manager/logger";

import { sleep } from "data_utils/time";

import * as ElementPaths from "./ElementPaths";

import { getSessionPage } from "../Session";

import PageNetworkUtils from "../PageNetworkUtils";

import { stateAbbreviationToName } from "data_utils/constants/stateAbbreviations";

import {
  validateNpmrdsDownloadRequestNetworkObjects,
  collectNpmrdsExportResponseObjects,
} from "./utils/npmrdsDataMonthExportRequestUtils";

import { getTimestamp } from "data_utils/time";

import { DataDate, DataDateRange } from "data_types/npmrds/domain";

import {
  createDataDate,
  createDataDateRange,
} from "data_types/npmrds/utils/dates";

import getEtlMetadataDir from "../../utils/getEtlMetadataDir";
import * as tmcsList from "../../utils/tmcsList";

import {
  NpmrdsTmc,
  NpmrdsState,
  NpmrdsDataYear,
  NpmrdsDownloadName,
} from "data_types/npmrds/domain";

import {
  RitisExportNpmrdsDataSource,
  NpmrdsDownloadRequest,
  NpmrdsTmcGeoJsonFeature,
  RitisExportRequestNetworkObjects,
  NpmrdsExportMetadata,
} from "../../domain";

import { setNpmrdsExportMetadataAsync } from "data_types/npmrds/utils/npmrds_export_metadata";

type MDDTmcMetadata = {
  firstname: string;
  is_nhs: boolean;
  roadname: string;
  roadnumber: string;
  tmc: string;
  miles: number;
  state: string;
  end_longitude: number;
  county: string;
  direction: string;
  start_longitude: string;
  type: string;
  road_order: number;
  func_class: string;
  zip: string;
  start_latitude: number;
  linear_id: number;
  end_latitude: number;
};

const gzipAsync = promisify(gzip);

const SECOND = 1000;
const ONE_MINUTE = SECOND * 60;
const SEVEN_MINUTES = ONE_MINUTE * 7;

export const TaskDoneEventType =
  "MassiveDataDownloader.requestNpmrdsDataExport:DONE";

export const SubmittedFormEventType =
  "MassiveDataDownloader.requestNpmrdsDataExport:SUBMITTED_FORM";

export function createNpmrdsDownloadName(
  state: NpmrdsState,
  is_expanded: boolean,
  date_range: DataDateRange,
  timestamp = getTimestamp(new Date())
): NpmrdsDownloadName {
  const { start, end } = date_range;

  if (start.year !== end.year) {
    throw new Error(
      "NPMRDS download date ranges MUST be within a calendar year."
    );
  }

  const prefix = is_expanded ? "npmrdsx" : "npmrds";

  const startYYYY = `${start.year}`;
  const startMM = `0${start.month}`.slice(-2);
  const startDD = `0${start.day}`.slice(-2);

  const startYYYYMMDD = `${startYYYY}${startMM}${startDD}`;

  const endYYYY = `${end.year}`;
  const endMM = `0${end.month}`.slice(-2);
  const endDD = `0${end.day}`.slice(-2);

  const endYYYYMMDD = `${endYYYY}${endMM}${endDD}`;

  if (startYYYY !== endYYYY) {
    throw new Error("NPMRDS download requests must be within a calendar yer.");
  }

  if (startYYYYMMDD > endYYYYMMDD) {
    throw new Error("Start date precedes end date");
  }

  const name = `${prefix}_${state}_from_${startYYYYMMDD}_to_${endYYYYMMDD}_v${timestamp}`;

  return name;
}

export function createNpmrdsDataRangeDownloadRequest(
  state: NpmrdsState,
  start_date: string,
  end_date: string,
  is_expanded: boolean
): NpmrdsDownloadRequest {
  console.log(
    JSON.stringify({ state, start_date, end_date, is_expanded }, null, 4)
  );

  const date_range = createDataDateRange(start_date, end_date);

  const name = createNpmrdsDownloadName(state, is_expanded, date_range);

  if (date_range.start.year !== date_range.end.year) {
    throw new Error(
      "NPMRDS download date ranges MUST be within a calendar year."
    );
  }

  const {
    start: { year },
  } = date_range;

  if (date_range.start.iso > date_range.end.iso) {
    throw new Error("Start date precedes end date");
  }

  return {
    name,
    year,
    state,
    is_expanded,
    date_range,
  };
}

export default class MassiveDataDownloader {
  private static readonly pageUrl =
    "https://npmrds.ritis.org/analytics/download/";

  private _page!: Page | null;
  private pageNetworkUtils!: PageNetworkUtils;

  private async _getPage(): Promise<Page> {
    if (!this._page) {
      const maxRetries = 10;

      let retries = 0;

      while (retries++ < maxRetries) {
        try {
          logger.debug("get session page");
          // NOTE: This assignment prevent infinite recursion in the method calls within this block.
          this._page = await getSessionPage();

          this.pageNetworkUtils = new PageNetworkUtils(this._page);

          if (logger.level === "debug") {
            const etl_metadata_dir = getEtlMetadataDir();

            const requests_path = join(
              etl_metadata_dir,
              "massive_data_downloader_network_requests.ndjson"
            );

            logger.debug(
              `MassiveDataDownloader logging network requests to ${requests_path}`
            );

            this.pageNetworkUtils.logRequests(requests_path);

            const responses_path = join(
              etl_metadata_dir,
              "massive_data_downloader_network_responses.ndjson"
            );

            logger.debug(
              `MassiveDataDownloader logging network responses to ${responses_path}`
            );

            this.pageNetworkUtils.logResponses(responses_path);
          }

          logger.debug("start MassiveDataDownloader goto page");

          await this._page.goto(MassiveDataDownloader.pageUrl, {
            timeout: SEVEN_MINUTES,
          });

          logger.debug("finished MassiveDataDownloader goto page");

          logger.debug(
            "start MassiveDataDownloader wait for page network idle"
          );

          await this._page.waitForNetworkIdle({ timeout: 0 });

          logger.debug(
            "finished MassiveDataDownloader wait for page network idle"
          );

          logger.debug("start MassiveDataDownloader wait for query form");

          await this._page.waitForSelector(ElementPaths.queryFormSelector, {
            timeout: SEVEN_MINUTES,
          });

          logger.debug("finished MassiveDataDownloader wait for query form");

          break;
        } catch (err) {
          logger.error((<Error>err).message);
          logger.error((<Error>err).stack);

          this._disconnectPage();

          if (retries === maxRetries) {
            throw new Error(
              "ERROR: Unable to get the MassiveDataDownloader page."
            );
          } else {
            logger.warn(
              `Attempt #${retries} failed connecting to MassiveDataDownloader page.`
            );
          }
        }
      }
    }

    return <Page>this._page;
  }

  async _disconnectPage() {
    logger.debug("MassiveDataDownloader _disconnectPage");
    if (this._page !== null) {
      await this._page?.close();

      this._page = null;
    }
  }

  // Wraps puppeteer's page.$
  //  See: https://pptr.dev/api/puppeteer.page._
  // TypeScript method overloading: https://stackoverflow.com/a/13212871
  private async $(
    selector: string,
    throwIfNotFound?: false
  ): Promise<ElementHandle | null>;
  private async $(
    selector: string,
    throwIfNotFound: true
  ): Promise<ElementHandle>;
  private async $(
    selector: string,
    throwIfNotFound: boolean = false // FIXME: Use this everywhere possible so we know when selectors no longer valid.
  ): Promise<ElementHandle | null> {
    const page = await this._getPage();

    // NOTE: ??? Do these cause a memory leak if we don't always call el.dispose() ???
    const el = await page.$(selector);

    if (throwIfNotFound && el === null) {
      const selectorName = Object.keys(ElementPaths).find(
        (k) => ElementPaths[k] === selector
      );

      const msg = selectorName
        ? `Element not found for ElementPaths.${selectorName}`
        : `Element not found for the following selector: ${selector}`;

      throw new Error(msg);
    }

    return throwIfNotFound ? <ElementHandle>el : el;
  }

  // https://pptr.dev/api/puppeteer.page.__
  private async $$(selector: string): Promise<ElementHandle[]> {
    const page = await this._getPage();

    const els = await page.$$(selector);

    return els;
  }

  async getRitisPdaAppStore() {
    /*
      In the RITIS MassiveDataDownloader page's index.html

        <script>
          // we can hydrate the store with values from the server
          window.PDA_APP_STORE = {
            ...
          }
        </script>

      Therefore, npmrdsDataDateRange IS NOT obtained from an API end point.
        Rather, it is statically hydrated from the server.
        Updating the values will require a page refresh.
    */
    const page = await this._getPage();
    const PDA_APP_STORE = <any>await page.evaluate("PDA_APP_STORE");

    delete PDA_APP_STORE.emailAddress;
    delete PDA_APP_STORE.mapboxApiToken;

    return PDA_APP_STORE;
  }

  async getNpmrdsDataDateExtent(): Promise<[string, string]> {
    const PDA_APP_STORE = await this.getRitisPdaAppStore();

    logger.silly(
      `MassiveDataDownloader PDA_APP_STORE ${inspect({ PDA_APP_STORE })}`
    );

    const expected_date_format_re = /^\d{4}-\d{2}-\d{2}$/;

    const dateExtent = PDA_APP_STORE?.rawDataSourcesFromServer
      .map(({ minimum_date, maximum_date }) => ({
        minimum_date,
        maximum_date,
      }))
      .reduce(
        (acc: null | [string, string], { minimum_date, maximum_date }) => {
          [minimum_date, maximum_date].forEach((date) => {
            if (!expected_date_format_re.test(date)) {
              throw new Error(
                `PDA_APP_STORE date format changed. Expected YYYY-MM-DD. Got ${date}`
              );
            }
          });

          if (acc === null) {
            return [minimum_date, maximum_date];
          }

          if (minimum_date < acc[0]) {
            acc[0] = minimum_date;
          }

          if (maximum_date > acc[0]) {
            acc[1] = maximum_date;
          }

          return acc;
        },
        null
      );

    return dateExtent;
  }

  async getNpmrdsInrixMapYear(): Promise<number> {
    const el = await this.$(ElementPaths.npmrdsInrixMapYearDropDown, true);

    const year: string = await el.evaluate((node) =>
      (<HTMLSpanElement>node).innerText.trim().slice(-4)
    );

    const yr = +year;

    if (!Number.isFinite(yr)) {
      throw new Error("Error parsing NPMRDS map year dropdown text.");
    }

    return yr;
  }

  private async npmrdsInrixMapYearDropDownIsOpen(): Promise<boolean> {
    const el = await this.$(ElementPaths.tmcDataSourceDropDownDiv, true);

    const isOpen = await el.evaluate((node) =>
      (<HTMLDivElement>node).classList.contains("is-open")
    );

    return isOpen;
  }

  private async openNpmrdsInrixMapYearDropDown() {
    const maxRetries = 10;

    const isOpen = await this.npmrdsInrixMapYearDropDownIsOpen();

    if (!isOpen) {
      const page = await this._getPage();
      let retries = 0;

      while (retries++ < maxRetries) {
        try {
          logger.debug(
            "start MassiveDataDownloader click npmrdsInrixMapYearDropDown"
          );
          await page.click(ElementPaths.npmrdsInrixMapYearDropDown);
          logger.debug(
            "finished MassiveDataDownloader click npmrdsInrixMapYearDropDown"
          );

          await sleep(500);

          if (await this.npmrdsInrixMapYearDropDownIsOpen()) {
            logger.debug("npmrdsInrixMapYearDropDown opened");
            await page.waitForSelector(
              ElementPaths.npmrdsInrixMapYearDropDownList,
              { timeout: 500 }
            );

            return;
          }

          logger.debug("npmrdsInrixMapYearDropDown did not open");
        } catch (err) {
          logger.error((<Error>err).message);
          logger.error((<Error>err).stack);

          logger.warn("retrying openNpmrdsInrixMapYearDropDown");
        }
      }

      throw new Error("Unable to open npmrdsInrixMapYearDropDown");
    }
  }

  getNpmrdsMapYearForDataYear(year: NpmrdsDataYear) {
    return year <= 2017 ? 2017 : year;
  }

  // Private because this MUST only be called by selectStateTmcsForYear
  private async _setNpmrdsInrixMapYear(year: NpmrdsDataYear) {
    // FIXME: Get the available map years from the dropdown list.
    const mapYear = this.getNpmrdsMapYearForDataYear(year);

    if (year !== mapYear) {
      logger.info(
        `Setting NPMRDS Map Year to ${mapYear} for requested data year ${year}.`
      );
    }

    const page = await this._getPage();
    await this.openNpmrdsInrixMapYearDropDown();

    const listEl = await this.$(
      ElementPaths.npmrdsInrixMapYearDropDownOptionsContainer,
      true
    );

    const yearOptionIdx: number = <number>await listEl.evaluate(
      <EvaluateFuncWith<Element, [number]>>((
        node: HTMLDivElement,
        reqYear: number
      ) => {
        const children = [...node.children];

        const regExp = new RegExp(`${reqYear}$`);

        const idx = children.findIndex(<(value: Element) => boolean>(
          ((c: HTMLDivElement) => regExp.test(c.innerText.trim()))
        ));

        return idx;
      }),
      mapYear
    );

    if (yearOptionIdx === -1) {
      throw new Error(`Could not find NPMRDS Map Year option for ${mapYear}`);
    }

    const yearOptionSelector =
      // Note: Need to increment by 1 since selector uses 1 div above DropDownOptionsContainer,
      ElementPaths.getNpmrdsInrixMapYearDropDownOptionByIndex(
        yearOptionIdx + 1
      );

    await page.click(yearOptionSelector);
  }

  // Expand NPMRDS to the Full TMC Network checkbox.

  private async expandedMapIsSelected() {
    const el = await this.$(ElementPaths.expandedMapCheckBox, true);

    const isSelected = await el.evaluate(
      <EvaluateFuncWith<Element, []>>((e: HTMLInputElement) => e.checked)
    );

    el.dispose();

    return isSelected;
  }

  // Private because this MUST only be called by selectStateTmcsForYear
  private async _selectExpandedMap() {
    if (!(await this.expandedMapIsSelected())) {
      const page = await this._getPage();
      await page.click(ElementPaths.expandedMapClickTarget);
    }
  }

  // Private because this MUST only be called by selectStateTmcsForYear
  async _deselectExpandedMap() {
    if (await this.expandedMapIsSelected()) {
      const page = await this._getPage();
      await page.click(ElementPaths.expandedMapClickTarget);
    }
  }

  // Road/Regions Selector
  private async getNumberOfSelectedRegions(throwIfMoreThanOne = false) {
    const els = await this.$$(ElementPaths.selectedRoadsListItem);

    const n = els ? els.length : 0;

    // INVARIANT: Can only select one region at a time.
    if (throwIfMoreThanOne && n > 1) {
      throw new Error(
        "INVARIANT VIOLATION: Only one state's TMCs can be selected at a time."
      );
    }

    els.forEach((el) => el.dispose());

    return n;
  }

  private async verifyThatSingleRegionIsSelected() {
    const n = await this.getNumberOfSelectedRegions(true);

    if (n < 1) {
      throw new Error("No regions selected.");
    }
  }

  private async selectedSegmentsPopupIsOpen() {
    const el = await this.$(ElementPaths.showSegmentIdsPopupTextArea, false);

    el?.dispose();

    return el !== null;
  }

  private async openSelectedSegmentsPopup(verifyRegionSelected = true) {
    if (await this.selectedSegmentsPopupIsOpen()) {
      return;
    }

    if (verifyRegionSelected) {
      await this.verifyThatSingleRegionIsSelected();
    }

    const page = await this._getPage();

    await page.click(ElementPaths.showSegementIdsButton);

    await page.waitForSelector(ElementPaths.showSegmentIdsPopupTextArea);
  }

  private async closeSelectedSegmentsPopup() {
    if (!(await this.selectedSegmentsPopupIsOpen())) {
      return;
    }

    const page = await this._getPage();

    await page.click(ElementPaths.closeShowSegmentIdsPopup);
  }

  private async isGatheringSegments() {
    const spinner1 = await this.$(ElementPaths.gatheringSegmentsSpinner);
    const spinner2 = await this.$(ElementPaths.addTmcSegmentCodesSpinner);

    spinner1?.dispose;
    spinner2?.dispose;

    return !(spinner1 === null && spinner2 === null);
  }

  private async waitUntilSegmentsGathered(timeout_ms: number = 300 * 1000) {
    // const page = await this._getPage();

    const waitMs = 1000;
    const maxRetries = timeout_ms / waitMs;

    let retryCount = 0;
    while (true) {
      if (++retryCount > maxRetries) {
        throw new Error(
          `Has been gathering segments for ${timeout_ms / 1000} seconds.`
        );
      }

      if (!(await this.isGatheringSegments())) {
        break;
      }

      await sleep(waitMs);
    }
  }

  async getSelectedTmcs(verifyRegionSelected = true) {
    await this.waitUntilSegmentsGathered();

    // const n = await this.getNumberOfSelectedRegions(true);

    // if (n === 0) {
    // return [];
    // }

    await this.openSelectedSegmentsPopup(verifyRegionSelected);

    const page = await this._getPage();

    await page.waitForSelector(ElementPaths.showSegmentIdsPopupTextArea);

    const el = await this.$(ElementPaths.showSegmentIdsPopupTextArea, true);

    const text = <string>(
      await el.evaluate(
        <EvaluateFuncWith<Element, []>>((e: HTMLTextAreaElement) => e.value)
      )
    );

    el.dispose();

    await this.closeSelectedSegmentsPopup();

    logger.silly(`showSegmentIdsPopupTextArea text: ${text}`);

    const tmcs = text
      .replace(/[^-+0-9A-Z]/gi, " ")
      .split(/ /g)
      .filter(Boolean);

    logger.info(`===> selectedTmcs.length: ${tmcs.length}`);

    return tmcs;
  }

  private async clearAllSelectedTMCs() {
    await this.waitUntilSegmentsGathered();

    const el = await this.$(ElementPaths.removeAllSelectedRoads);

    if (el === null) {
      return;
    }

    const page = await this._getPage();

    let retries = 0;
    while (true) {
      if (++retries > 3) {
        throw new Error("Could not remove the selected TMCs.");
      }

      await el.click({ clickCount: 2 });

      try {
        await page.waitForSelector(ElementPaths.confirmRemoveAllSelectedRoads, {
          timeout: 250,
        });
        break;
      } catch (err) {
        logger.warn("No confirmRemoveAllSelectedRoads popup.");
      }

      el.dispose();

      const numOfSelectedRegions = await this.getNumberOfSelectedRegions();

      if (numOfSelectedRegions === 0) {
        break;
      }
    }

    await page.click(ElementPaths.confirmRemoveAllSelectedRoads);
  }

  private async regionTmcsSearchTabIsOpen() {
    const el = await this.$(ElementPaths.regionSelectorsList);

    el?.dispose();

    return el !== null;
  }

  private async openRegionTmcsSearchTab() {
    if (!(await this.regionTmcsSearchTabIsOpen())) {
      const page = await this._getPage();
      await page.click(ElementPaths.tmcsSearchRegionTab);
      await page.waitForSelector(ElementPaths.regionSelectorsList);
    }
  }

  private async stateRegionsDropListIsOpen() {
    const el = await this.$(ElementPaths.openedRegionsSelectorsList);

    if (el !== null) {
      el.dispose();
      return true;
    }

    return false;
  }

  private async openStateRegionsDropdownList() {
    await this.openRegionTmcsSearchTab();

    if (!(await this.stateRegionsDropListIsOpen())) {
      const page = await this._getPage();
      await page.click(ElementPaths.regionSelectorsList);
      await page.waitForSelector(ElementPaths.openedRegionsSelectorsList);
    }
  }

  private async closeStateRegionsDropdownList() {
    const el = await this.$(ElementPaths.openedRegionsSelectorsList);

    if (el !== null) {
      await el.click();
      el.dispose();
    }
  }

  private async clearSelectedRegions() {
    const listWasOpen = this.stateRegionsDropListIsOpen();

    await this.openStateRegionsDropdownList();

    const checkBoxEl = await this.$(ElementPaths.allRegionsCheckBox, true);
    const clickTargetEl = await this.$(
      ElementPaths.allRegionsClickTarget,
      true
    );

    let retries = 0;
    do {
      if (++retries > 3) {
        throw new Error("Unable to clear selected regions");
      }
      await clickTargetEl.click();
    } while (
      await checkBoxEl.evaluate(
        <EvaluateFuncWith<Element, []>>(
          ((node: HTMLInputElement) => node.checked)
        )
      )
    );

    checkBoxEl.dispose();
    clickTargetEl.dispose();

    if (!listWasOpen) {
      await this.closeStateRegionsDropdownList();
    }
  }

  // The problem with this method is that it is not pure.
  //   The data it returns depends on external state. (Form & DB)
  //   It also modifies external state (Form & DB)
  async selectStateTmcsForYear(
    year: NpmrdsDataYear,
    state: NpmrdsState,
    expandedMap = true,
    retries = 0
  ): Promise<NpmrdsTmc[]> {
    if (retries > 3) {
      throw new Error("ERROR: unable to select state region.");
    }

    await this._setNpmrdsInrixMapYear(year);

    await this.clearAllSelectedTMCs();

    if (expandedMap) {
      await this._selectExpandedMap();
    } else {
      await this._deselectExpandedMap();
    }

    await this.openStateRegionsDropdownList();

    await this.clearSelectedRegions();

    const stateName = stateAbbreviationToName[state];
    const stateNameNormalized = stateName.toLowerCase().replace(/[^a-z ]/g, "");

    const stateSelectors = await this.$$(ElementPaths.stateRegionsListOptions);

    let stateDropListOptionEl: ElementHandle;

    // Search for the stateDropListOptionEl
    for (stateDropListOptionEl of stateSelectors) {
      const text = <string>(
        await stateDropListOptionEl.evaluate(
          <EvaluateFuncWith<Element, []>>(
            ((node: HTMLDivElement) => node.innerText)
          )
        )
      );

      const textNormalized = text
        .trim()
        .toLowerCase()
        .replace(/[^a-z ]/g, "");

      if (stateNameNormalized === textNormalized) {
        break;
      }
    }

    const page = await this._getPage();

    // @ts-ignore
    await stateDropListOptionEl.click();

    logger.debug(
      "start MassiveDataDownloader wait for addRegionSelectedRoadsButton"
    );
    await page.waitForSelector(ElementPaths.addRegionSelectedRoadsButton);
    logger.debug(
      "finished MassiveDataDownloader wait for addRegionSelectedRoadsButton"
    );

    //  Sometimes the counties selector covers the addRegionSelectedRoadsButton.
    //    Clicking the title div closes the counties selector.
    await page.click(ElementPaths.pageTitle);

    await page.click(ElementPaths.addRegionSelectedRoadsButton);

    await sleep(3000);

    const warningTextAlert = await this.$(
      ElementPaths.pleaseChooseAtLeastOneStateWarningText
    );

    if (warningTextAlert !== null) {
      const innerText = await warningTextAlert.evaluate(<
        EvaluateFuncWith<Element, []>
      >((e: HTMLInputElement) => e.innerText.trim().toLowerCase()));

      if (
        innerText !== "please choose at least one state, county, or zip code."
      ) {
        throw new Error("Unrecognized warning alert box.");
      }

      logger.warn(
        'WARNING: Got the "Please choose at least one state, county, or zip code." alert.'
      );

      await page.click(ElementPaths.pleaseChooseAtLeastOneStateAlertButton);
      return this.selectStateTmcsForYear(year, state, expandedMap, ++retries);
    }

    await this.waitUntilSegmentsGathered();

    await this.clearSelectedRegions();
    await this.closeStateRegionsDropdownList();

    const tmcs = await this.getSelectedTmcs();

    return tmcs;
  }

  private async tmcSegmentCodesTabIsOpen() {
    const el = await this.$(ElementPaths.tmcsSegmentCodesTextArea);

    return el !== null;
  }

  private async openTmcsSegmentCodesTab() {
    if (!(await this.tmcSegmentCodesTabIsOpen())) {
      const page = await this._getPage();
      logger.debug("start MassiveDataDownloader click tmcsSegmentCodesTab");
      await page.click(ElementPaths.tmcsSegmentCodesTab);
      logger.debug("finished MassiveDataDownloader click tmcsSegmentCodesTab");

      logger.debug(
        "start MassiveDataDownloader wait for tmcsSegmentCodesTextArea"
      );
      await page.waitForSelector(ElementPaths.tmcsSegmentCodesTextArea);
      logger.debug(
        "finished MassiveDataDownloader wait for tmcsSegmentCodesTextArea"
      );
    }
  }

  // This method is used to collect the TMC GeoJSON Features.
  // It will also be required for requesting Canadian TMCs.
  async selectSpecificTmcsForYear(
    year: NpmrdsDataYear,
    tmcs: NpmrdsTmc[]
    // enableAutoRefreshMap = true
  ) {
    await this._setNpmrdsInrixMapYear(year);
    await this._selectExpandedMap();
    await this.clearAllSelectedTMCs();

    logger.debug("start MassiveDataDownloader open TMCs SegmentCodes Tab");
    await this.openTmcsSegmentCodesTab();
    logger.debug("finished MassiveDataDownloader open TMCs SegmentCodes Tab");

    const page = await this._getPage();

    // if (enableAutoRefreshMap) {
    // const tmcSegmentCodesAutoRefreshCheckBoxEl = await this.$(
    // ElementPaths.allRegionsCheckBox
    // );
    // const tmcSegmentCodesAutoRefreshEnabled =
    // await tmcSegmentCodesAutoRefreshCheckBoxEl.evaluate(
    // (e: HTMLInputElement) => e.checked
    // );

    // if (!tmcSegmentCodesAutoRefreshEnabled) {
    // await page.click(ElementPaths.autoRefreshMapClickTarget);
    // }
    // }

    logger.debug(
      "start MassiveDataDownloader type TMCs into tmcsSegmentCodesTextArea"
    );

    // https://stackoverflow.com/q/56561589/3970755
    await page.click(ElementPaths.tmcsSegmentCodesTextArea, { clickCount: 3 });
    await page.evaluate((_tmcs) => {
      navigator.clipboard.writeText(`${_tmcs}`);
    }, tmcs);

    await sleep(SECOND);

    await page.keyboard.down("Control");
    await page.keyboard.press("V");
    await page.keyboard.up("Control");

    logger.debug(
      "finished MassiveDataDownloader type TMCs into tmcsSegmentCodesTextArea"
    );

    logger.debug("start MassiveDataDownloader click addTmcSegmentCodes");

    await page.click(ElementPaths.addTmcSegmentCodesButton);

    logger.debug("finish click addTmcSegmentCodes");

    logger.debug("start MassiveDataDownloader get selected TMCs");
    let selectedTmcs: NpmrdsTmc[] = [];

    let retries = 0;
    while (!selectedTmcs?.length && ++retries <= 5) {
      try {
        logger.debug(`Getting the selected TMCs attempt # ${retries}`);

        await this.waitUntilSegmentsGathered();
        selectedTmcs = await this.getSelectedTmcs(false);
      } catch (err) {
        logger.error((<Error>err).message);
        await sleep(SECOND * 10);
      }
    }

    logger.debug("finished MassiveDataDownloader get selected TMCs");

    const actualTmcsSet = new Set(selectedTmcs);
    const expectedTmcsSet = new Set(tmcs);

    const setEquality =
      tmcs.every((tmc) => actualTmcsSet.has(tmc)) &&
      selectedTmcs.every((tmc) => expectedTmcsSet.has(tmc));

    if (!setEquality) {
      const selectedNotRequested = _.difference(selectedTmcs, tmcs);
      const requestedNotSelected = _.difference(tmcs, selectedTmcs);

      logger.silly(
        JSON.stringify(
          {
            // selectedTmcs,
            // tmcs,
            selectedNotRequested,
            requestedNotSelected,
            selectedNotRequestedLength: selectedNotRequested.length,
            requestedNotSelectedLength: requestedNotSelected.length,
          },
          null,
          4
        )
      );

      throw new Error("Unable to select the requested TMCs");
    }

    return tmcs;
  }

  async collectTmcGeoJsonFeaturesFromRoadCoordinatesResponse() {
    const page = await this._getPage();

    return await new Promise<Record<NpmrdsTmc, NpmrdsTmcGeoJsonFeature>>(
      (resolve) => {
        const tmcFeaturesCollector = async (event: HTTPResponse) => {
          const res = await PageNetworkUtils.parseHttpResponseEvent(event);

          if (
            res.response_url ===
            "https://npmrds.ritis.org/api/road_coordinates/"
          ) {
            const {
              geojson,
              tmcs: tmcsMetadata,
            }: { geojson: turf.FeatureCollection; tmcs: MDDTmcMetadata[] } =
              res.response_body;

            const geometriesByTmc = geojson.features.reduce(
              // @ts-ignore
              (
                acc: Record<
                  string,
                  turf.Feature<turf.LineString | turf.MultiLineString>
                >,
                feature: turf.Feature<turf.LineString | turf.MultiLineString>
              ) => {
                const { id } = feature;
                // @ts-ignore
                acc[id] = feature;
                return acc;
              },
              {}
            );

            const tmcsFeatures: Record<NpmrdsTmc, NpmrdsTmcGeoJsonFeature> = {};

            for (const meta of tmcsMetadata) {
              const { tmc } = meta;

              const geom = geometriesByTmc[tmc];

              const { geometry: { type = null, coordinates = null } = {} } =
                geom || {};

              if (coordinates !== null) {
                if (type === "LineString") {
                  // @ts-ignore
                  tmcsFeatures[tmc] = turf.lineString(coordinates, meta, {
                    id: tmc,
                  });
                }

                if (type === "MultiLineString") {
                  // @ts-ignore
                  tmcsFeatures[tmc] = turf.multiLineString(coordinates, meta, {
                    id: tmc,
                  });
                }
              }
            }

            page.off("response", tmcFeaturesCollector);

            return resolve(tmcsFeatures);
          }
        };
        page.on("response", tmcFeaturesCollector);
      }
    );
  }

  // FIXME: Make this an AsyncGenerator
  async *collectTmcGeoJsonFeatures(
    state: NpmrdsState,
    year: NpmrdsDataYear,
    expandedMap = true
  ): AsyncGenerator<NpmrdsTmcGeoJsonFeature> {
    logger.debug("started clearAllSelectedTMCs");
    await this.clearAllSelectedTMCs();
    logger.debug("finished MassiveDataDownloader clearAllSelectedTMCs");

    console.log("featuresByTmcPromise");
    const featuresByTmcPromise =
      this.collectTmcGeoJsonFeaturesFromRoadCoordinatesResponse();

    console.log("selectStateTmcsForYear");
    console.time("selectStateTmcsForYear");
    const tmcs = await this.selectStateTmcsForYear(year, state, expandedMap);
    console.timeEnd("selectStateTmcsForYear");

    console.log("await featuresByTmc");
    console.time("await featuresByTmc");

    const featuresByTmc = await featuresByTmcPromise;

    console.timeEnd("await featuresByTmc");

    for (const tmc of Object.keys(featuresByTmc).sort()) {
      yield featuresByTmc[tmc];
    }

    const tmcsWithGeometriesSet = new Set(Object.keys(featuresByTmc));

    const tmcsWithoutGeometries = tmcs.filter(
      (tmc) => !tmcsWithGeometriesSet.has(tmc)
    );

    if (tmcsWithoutGeometries.length) {
      const chunks = _.chunk(tmcsWithoutGeometries, 5000);

      for (const chunk of chunks) {
        await sleep(ONE_MINUTE);

        const batchFeaturesByTmcPromise =
          this.collectTmcGeoJsonFeaturesFromRoadCoordinatesResponse();

        await this.selectSpecificTmcsForYear(year, chunk);

        const batchFeaturesByTmc = await batchFeaturesByTmcPromise;

        for (const tmc of Object.keys(batchFeaturesByTmc).sort()) {
          logger.silly(`yielding GeoJSON for ${tmc}`);
          yield batchFeaturesByTmc[tmc];
        }
      }
    }
  }

  // Select one or more date ranges
  async getSelectedStartDate(): Promise<DataDate> {
    const el = await this.$(ElementPaths.lowerDateOpenCalendarTarget, true);

    const selectedDate: string = await el.evaluate(
      (node) => (<HTMLInputElement>node).value
    );

    const [mm, dd, yyyy] = selectedDate.split(/\//g);

    const iso_date = `${yyyy}-${mm}-${dd}`;

    const data_date = createDataDate(iso_date, "start");

    return data_date;
  }

  private async openStartDateCalendar() {
    if (!(await this.$(ElementPaths.lowerDateDatePicker))) {
      const page = await this._getPage();

      await page.click(ElementPaths.lowerDateOpenCalendarTarget);
      await page.waitForSelector(ElementPaths.lowerDateDatePicker);
    }
  }

  async setSelectedStartDate(startDate: DataDate) {
    const page = await this._getPage();

    await this.openStartDateCalendar();

    await page.select(ElementPaths.lowerDateYearSelector, `${startDate.year}`);
    await page.select(
      ElementPaths.lowerDateMonthSelector,
      `${startDate.month - 1}`
    );

    const dayEls = await this.$$(ElementPaths.lowerDateDaysMultiSelector);

    for (const dayOfMonthEl of dayEls) {
      const found = await dayOfMonthEl.evaluate(
        <EvaluateFuncWith<Element, [number]>>(
          ((node: HTMLOptionElement, date: number) =>
            ![...node.classList].some((c) => /--outside-month$/.test(c)) &&
            node.getAttribute("role") === "option" &&
            +node.innerText.trim() === date)
        ),
        startDate.day
      );

      if (found) {
        await dayOfMonthEl.click();
        break;
      }
    }

    dayEls.forEach((el) => el.dispose);
  }

  async getSelectedEndDate(): Promise<DataDate> {
    const el = await this.$(ElementPaths.lowerDateOpenCalendarTarget, true);

    const selectedDate: string = await el.evaluate(
      (node) => (<HTMLInputElement>node).value
    );

    const [mm, dd, yyyy] = selectedDate.split(/\//g);

    const iso_date = `${yyyy}-${mm}-${dd}`;

    const data_date = createDataDate(iso_date, "end");

    logger.debug(
      `MassiveDataDownloader selected end date: ${JSON.stringify(
        data_date,
        null,
        4
      )}`
    );

    return data_date;
  }

  private async openEndDateCalendar() {
    if (!(await this.$(ElementPaths.upperDateDatePicker))) {
      const page = await this._getPage();

      await page.click(ElementPaths.upperDateOpenCalendarTarget);
      await page.waitForSelector(ElementPaths.upperDateDatePicker);
    }
  }

  async setSelectedEndDate(endDate: DataDate) {
    const page = await this._getPage();

    await this.openEndDateCalendar();

    await page.select(ElementPaths.upperDateYearSelector, `${endDate.year}`);
    await page.select(
      ElementPaths.upperDateMonthSelector,
      `${endDate.month - 1}`
    );

    const dayEls = await this.$$(ElementPaths.upperDateDaysMultiSelector);

    let found = false;
    for (const dayOfMonthEl of dayEls) {
      found = <boolean>(
        await dayOfMonthEl.evaluate(
          <EvaluateFuncWith<Element, [number]>>(
            ((node: HTMLOptionElement, date: number) =>
              ![...node.classList].some((c) => /--outside-month$/.test(c)) &&
              node.getAttribute("role") === "option" &&
              +node.innerText.trim() === date)
          ),
          endDate.day
        )
      );

      if (found) {
        await dayOfMonthEl.click();
        break;
      }
    }

    if (!found) {
      throw new Error(`Unable to find date: ${JSON.stringify(endDate)}`);
    }

    dayEls.forEach((el) => el.dispose);
  }

  // Select days of the week

  async selectAllDaysOfTheWeek() {
    const dowSelectorPathsByDow = ElementPaths.selectDayOfWeekButtons;

    for (const selector of Object.values(dowSelectorPathsByDow)) {
      const el = await this.$(selector, true);

      const selected = await el.evaluate(<EvaluateFuncWith<Element, []>>(
        ((node) => [...node.classList].some((c) => /selected/.test(c)))
      ));

      if (!selected) {
        await el.click();
      }
    }
  }

  // Select one or more times of day
  async selectFullDay() {
    const startTimeEl = await this.$(ElementPaths.startTime, true);

    const startTimeValue = await startTimeEl.evaluate(
      <EvaluateFuncWith<Element, []>>((node: HTMLInputElement) => node.value)
    );

    const startTime = "12:00";

    if (startTimeValue !== startTime) {
      await startTimeEl.click({ clickCount: 3 });
      await startTimeEl.type(startTime, { delay: 50 });
    }

    const startMeridiemEl = await this.$(ElementPaths.startTimeMeridiem, true);

    const startMeridiemValue = await startMeridiemEl.evaluate(
      <EvaluateFuncWith<Element, []>>((node: HTMLSelectElement) => node.value)
    );

    const startMeridiem = "AM";

    if (startMeridiemValue !== startMeridiem) {
      await startMeridiemEl.select(startMeridiem);
    }

    const endTimeEl = await this.$(ElementPaths.endTime, true);
    const endTimeValue = await endTimeEl.evaluate(
      <EvaluateFuncWith<Element, []>>((node: HTMLInputElement) => node.value)
    );

    const endTime = "11:59";

    if (endTimeValue !== endTime) {
      await endTimeEl.click({ clickCount: 3 });
      await endTimeEl.type(endTime, { delay: 50 });
    }

    const endMeridiemEl = await this.$(ElementPaths.endTimeMeridiem, true);
    const endMeridiemValue = await endMeridiemEl.evaluate(
      <EvaluateFuncWith<Element, []>>((node: HTMLSelectElement) => node.value)
    );

    const endMeridiem = "PM";

    if (endMeridiemValue !== endMeridiem) {
      await endMeridiemEl.select(endMeridiem);
    }
  }

  async selectNpmrdsDataDateRange(startDate: DataDate, endDate: DataDate) {
    await this.setSelectedStartDate(startDate);
    await this.setSelectedEndDate(endDate);
    await this.selectAllDaysOfTheWeek();
    await this.selectFullDay();
  }

  // Select data sources and measures

  async completePassengerVehicleMeasuresAreSelected() {
    const el = await this.$(
      ElementPaths.passengerVehiclesDataSourceRootCheckbox,
      true
    );

    const completeSelected = await el.evaluate(<EvaluateFuncWith<Element, []>>(
      ((e: HTMLSpanElement) => [...e.classList].some((c) => /checked$/.test(c)))
    ));

    return completeSelected;
  }

  async selectCompletePassengerVehicleMeasures() {
    const page = await this._getPage();

    let count = 0;
    while (true) {
      if (await this.completePassengerVehicleMeasuresAreSelected()) {
        break;
      }

      if (++count > 3) {
        throw new Error(
          "Unable to select complete passenger vehicle measures."
        );
      }

      await page.click(ElementPaths.passengerVehiclesDataSourceClickTarget);
    }
  }

  async completeAllVehicleMeasuresAreSelected() {
    const el = await this.$(
      ElementPaths.allVehiclesDataSourceRootCheckbox,
      true
    );

    const completeSelected = await el.evaluate(<EvaluateFuncWith<Element, []>>(
      ((e: HTMLSpanElement) => [...e.classList].some((c) => /checked$/.test(c)))
    ));

    return completeSelected;
  }

  async selectCompleteAllVehicleMeasures() {
    const page = await this._getPage();

    let count = 0;
    while (true) {
      if (await this.completeAllVehicleMeasuresAreSelected()) {
        break;
      }

      if (++count > 3) {
        throw new Error(
          "Unable to select complete passenger vehicle measures."
        );
      }

      await page.click(ElementPaths.allVehiclesDataSourceClickTarget);
    }
  }

  async completeTruckMeasuresAreSelected() {
    const el = await this.$(ElementPaths.trucksDataSourceRootCheckbox, true);

    const completeSelected = await el.evaluate(<EvaluateFuncWith<Element, []>>(
      ((e: HTMLSpanElement) => [...e.classList].some((c) => /checked$/.test(c)))
    ));

    return completeSelected;
  }

  async selectCompleteTruckMeasures() {
    const page = await this._getPage();

    let count = 0;
    while (true) {
      if (await this.completeTruckMeasuresAreSelected()) {
        break;
      }

      if (++count > 3) {
        throw new Error(
          "Unable to select complete passenger vehicle measures."
        );
      }

      await page.click(ElementPaths.trucksDataSourceClickTarget);
    }
  }

  async selectAllMeasures() {
    await this.selectCompletePassengerVehicleMeasures();
    await this.selectCompleteAllVehicleMeasures();
    await this.selectCompleteTruckMeasures();
  }

  // Select units for travel time

  async timeUnitIsSeconds() {
    const el = await this.$(ElementPaths.timeUnitsSecondsRadioButton, true);

    const secondsSelected = await el.evaluate(
      <EvaluateFuncWith<Element, []>>((e: HTMLInputElement) => e.checked)
    );

    return secondsSelected;
  }

  async setTimeUnitToSeconds() {
    if (!(await this.timeUnitIsSeconds())) {
      const page = await this._getPage();
      await page.click(ElementPaths.timeUnitSecondsClickTarget);
    }
  }

  // Include NULL records

  async includeNullRecordsIsSelected() {
    const el = await this.$(ElementPaths.includeNullRecordsCheckbox, true);

    const includeNulls = await el.evaluate(
      <EvaluateFuncWith<Element, []>>((e: HTMLInputElement) => e.checked)
    );

    return includeNulls;
  }

  async selectIncludeNullValues() {
    if (!(await this.includeNullRecordsIsSelected())) {
      const page = await this._getPage();
      await page.click(ElementPaths.includeNullRecordsClickTarget);
    }
  }

  // Select averaging

  async dontAverageTravelTimesIsSelected() {
    const el = await this.$(
      ElementPaths.dontAverageTravelTimesRadioButton,
      true
    );

    const dontAvgSelected = await el.evaluate(
      <EvaluateFuncWith<Element, []>>((e: HTMLInputElement) => e.checked)
    );

    return dontAvgSelected;
  }

  async selectDontAverageTravelTimes() {
    if (!(await this.dontAverageTravelTimesIsSelected())) {
      const page = await this._getPage();
      await page.click(ElementPaths.dontAverageTravelTimesClickTarget);
    }
  }

  // Provide title
  async getDownloadNameInTitleField() {
    const el = await this.$(ElementPaths.downloadNameField, true);

    const downloadName: string = await el.evaluate(
      (node) => (<HTMLInputElement>node).value
    );

    return downloadName;
  }

  async setDownloadNameInTitleField(downloadName: NpmrdsDownloadName) {
    const page = await this._getPage();

    // https://stackoverflow.com/a/52633235/3970755
    await page.click(ElementPaths.downloadNameField, { clickCount: 3 });

    await page.type(ElementPaths.downloadNameField, `${downloadName}`, {
      delay: 50,
    });
  }

  // Notification

  async notificationsAreDisabled() {
    const el = await this.$(ElementPaths.sendNotificationsCheckBox, true);

    const checked = await el.evaluate(
      <EvaluateFuncWith<Element, []>>((e: HTMLInputElement) => e.checked)
    );

    return !checked;
  }

  async disableNotifications() {
    if (!(await this.notificationsAreDisabled())) {
      const page = await this._getPage();
      await page.click(ElementPaths.sendNotificationsClickTarget);
    }
  }

  private async selectCompleteDownloadOptions() {
    await this.selectAllDaysOfTheWeek();
    await this.selectFullDay();
    await this.selectAllMeasures();
    await this.setTimeUnitToSeconds();
    await this.selectIncludeNullValues();
    await this.selectDontAverageTravelTimes();
    await this.disableNotifications();
  }

  private async validateRequestedNpmrdsDataDateRange(
    date_range: DataDateRange
  ) {
    const { start, end } = date_range;

    if (start.iso_date > end.iso_date) {
      throw new Error(
        `Requested end_date ${end.iso_date} precedes start_date ${start.iso_date}.`
      );
    }

    const requestedDateErrors: string[] = [];

    const [min_data_date, max_data_date] = await this.getNpmrdsDataDateExtent();

    if (start.iso_date < min_data_date) {
      requestedDateErrors.push(
        `The requested start date ${start.iso_date} precedes the earliest NPMRDS data date ${min_data_date}.`
      );
    }

    if (end.iso_date > max_data_date) {
      requestedDateErrors.push(
        `The requested end date ${end.iso_date} follows the latest NPMRDS data date ${max_data_date}.`
      );
    }

    if (requestedDateErrors.length) {
      const plural = requestedDateErrors.length > 1 ? "s" : "";
      throw new Error(
        `Requested Date Range Error${plural}: ${requestedDateErrors}`
      );
    }
  }

  private async fillOutFormForNpmrdsExportRequest(
    npmrdsDownloadRequest: NpmrdsDownloadRequest
  ) {
    const { state, year, name, date_range } = npmrdsDownloadRequest;

    if (name === null) {
      throw new Error("NPMRDS download request name is required");
    }

    this.validateRequestedNpmrdsDataDateRange(date_range);

    logger.debug("selecting state TMCs for year...");
    const tmcs = await this.selectStateTmcsForYear(
      year,
      state,
      npmrdsDownloadRequest.is_expanded
    );
    logger.debug("...selected state TMCs for year");

    await tmcsList.setTmcs(tmcs);

    await this.selectNpmrdsDataDateRange(date_range.start, date_range.end);

    await this.setDownloadNameInTitleField(name);

    await this.selectCompleteDownloadOptions();
  }

  private async clickSubmitButton() {
    const page = await this._getPage();
    await page.click(ElementPaths.submitButton);
  }

  private async clearSubmittedFormModal() {
    const page = await this._getPage();
    await page.waitForSelector(ElementPaths.formSubmittedModalOkButton);
    await page.click(ElementPaths.formSubmittedModalOkButton);
  }

  async requestNpmrdsDataExport(
    state: NpmrdsState,
    start_date: string,
    end_date: string,
    is_expanded = true
  ): Promise<RitisExportRequestNetworkObjects> {
    verifyIsInTaskEtlContext();

    try {
      const events = await dama_events.getAllEtlContextEvents();

      let task_done_event = events.find(
        ({ type }) => type === TaskDoneEventType
      );

      if (task_done_event) {
        return task_done_event.payload;
      }

      const submitted_form_event = events.find(
        ({ type }) => type === SubmittedFormEventType
      );

      if (submitted_form_event) {
        // TODO: Come up with a mechanism to test if the request went through and react accordingly.
        throw new Error(
          "Previously failed after submitting form. Throwing error rather submitting duplicate request."
        );
      }

      const npmrds_download_request = createNpmrdsDataRangeDownloadRequest(
        state,
        start_date,
        end_date,
        is_expanded
      );

      logger.debug("start MassiveDataDownloader requestNpmrdsDataExport");

      const initialPage = await this._getPage();

      logger.debug(
        "start MassiveDataDownloader filling out OutFormForNpmrdsDateRangeRequest"
      );
      await this.fillOutFormForNpmrdsExportRequest(npmrds_download_request);
      logger.debug(
        "finished MassiveDataDownloader filling out OutFormForNpmrdsDateRangeRequest"
      );

      logger.debug("started validating NpmrdsDownloadRequestNetworkObjects");
      const validateRequestsPromise =
        validateNpmrdsDownloadRequestNetworkObjects(
          this.pageNetworkUtils,
          npmrds_download_request
        );
      logger.debug(
        "finished MassiveDataDownloader validating NpmrdsDownloadRequestNetworkObjects"
      );

      if (initialPage === (await this._getPage())) {
        logger.debug("start MassiveDataDownloader click submit button");
        await this.clickSubmitButton();
        logger.debug("finished MassiveDataDownloader click submit button");
      } else {
        throw new Error(
          "Reconnected to the page while in the middle of filling out the form."
        );
      }

      const {
        error: validateRequestsError,
        validationErrorsByDataSource,
        proceed,
        abort,
      } = await validateRequestsPromise;

      logger.debug(
        inspect(
          {
            validationErrorsByDataSource,
          },
          { colors: false, sorted: true }
        )
      );

      // This is a fatal error. Burn everything down.
      if (validateRequestsError) {
        await abort?.();
        throw validateRequestsError;
      }

      // Something's wrong with the form. Abort but keep the page open.
      if (
        validationErrorsByDataSource &&
        Object.values(validationErrorsByDataSource).some(Boolean)
      ) {
        await abort?.();
        throw new Error(JSON.stringify(validationErrorsByDataSource, null, 4));
      }

      // calls continue on all the export requests
      logger.debug("validation passed, proceeding.");

      if (!proceed) {
        throw new Error(
          "VALIDATION INVARIANT BROKEN: proceed function is null"
        );
      }

      // NOTE: collecting responses before validation seems to break abort/proceed.
      const collectResponsesPromise = collectNpmrdsExportResponseObjects(
        this.pageNetworkUtils,
        npmrds_download_request
      );

      await proceed();

      await dama_events.dispatch({
        type: "MassiveDataDownloader.requestNpmrdsDataExport:SUBMITTED_FORM",
        payload: { npmrds_download_request },
      });

      logger.debug("dispatched :SUBMITTED_FORM event");

      logger.debug(
        "start MassiveDataDownloader collect NpmrdsExportResponseObjects"
      );

      const exportResponsesByDataSource = await collectResponsesPromise;

      logger.debug("finished collect NpmrdsExportResponseObjects");

      const ritis_export_request_network_objects: RitisExportRequestNetworkObjects =
        {
          all_vehicles_request:
            exportResponsesByDataSource[
              RitisExportNpmrdsDataSource.ALL_VEHICLES
            ]?.request_body || null,
          all_vehicles_response:
            exportResponsesByDataSource[
              RitisExportNpmrdsDataSource.ALL_VEHICLES
            ]?.response_body || null,

          passenger_vehicles_request:
            exportResponsesByDataSource[
              RitisExportNpmrdsDataSource.PASSENGER_VEHICLES
            ]?.request_body || null,
          passenger_vehicles_response:
            exportResponsesByDataSource[
              RitisExportNpmrdsDataSource.PASSENGER_VEHICLES
            ]?.response_body || null,

          trucks_request:
            exportResponsesByDataSource[RitisExportNpmrdsDataSource.TRUCKS]
              ?.request_body || null,
          trucks_response:
            exportResponsesByDataSource[RitisExportNpmrdsDataSource.TRUCKS]
              ?.response_body || null,
        };

      if (Object.values(ritis_export_request_network_objects).some((v) => !v)) {
        throw new Error(
          `INVARIANT BROKEN: ritis_export_request_network_objects not fully populated ${JSON.stringify(
            ritis_export_request_network_objects,
            null,
            4
          )}`
        );
      }

      task_done_event = {
        type: "MassiveDataDownloader.requestNpmrdsDataExport:DONE",
        payload: _.mapValues(ritis_export_request_network_objects, (v) => {
          v = _.cloneDeep(v);

          const truncated_msg = "TMCs list truncated for event_store storage";

          // @ts-ignore
          if (Array.isArray(v.TMCS)) {
            // @ts-ignore
            v.TMCS = truncated_msg;
          }

          // @ts-ignore
          if (Array.isArray(v.arguments?.tmcs)) {
            // @ts-ignore
            v.arguments.tmcs = truncated_msg;
          }

          // @ts-ignore
          if (Array.isArray(v.ROAD_DETAILS?.[0].SEGMENT_IDS)) {
            // @ts-ignore
            v.ROAD_DETAILS[0].SEGMENT_IDS = truncated_msg;
          }
          // @ts-ignore
          if (Array.isArray(v.arguments?.road_details?.[0].SEGMENT_IDS)) {
            // @ts-ignore
            v.arguments.road_details[0].SEGMENT_IDS = truncated_msg;
          }

          return v;
        }),
      };

      await dama_events.dispatch(task_done_event);

      this.writeRitisExportRequestNetworkObjects(
        ritis_export_request_network_objects
      );

      const { year, date_range } = npmrds_download_request;

      const is_complete_month =
        date_range.start.is_start_of_month && date_range.end.is_end_of_month;

      const is_complete_week =
        date_range.start.is_start_of_week && date_range.end.is_end_of_week;

      const npmrds_export_metadata: NpmrdsExportMetadata = {
        name: npmrds_download_request.name,
        state,
        year,
        start_date,
        end_date,
        is_expanded,
        is_complete_month,
        is_complete_week,
        pg_env: getPgEnv(),
        etl_context_id: getEtlContextId()!,
        parent_context_id: await dama_events.getParentEtlContextId(),
      };

      await setNpmrdsExportMetadataAsync(npmrds_export_metadata);
      await this.writePdaAppStore();

      logger.debug("start MassiveDataDownloader clearSubmittedFormModal");
      await this.clearSubmittedFormModal();
      logger.debug("finished MassiveDataDownloader clearSubmittedFormModal");

      logger.debug("finished requestNpmrdsDataExport");

      return ritis_export_request_network_objects;
    } finally {
      await sleep(5 * SECOND);

      await this._disconnectPage();
    }
  }

  async writePdaAppStore() {
    const PDA_APP_STORE = await this.getRitisPdaAppStore();

    const etl_metadata_dir = getEtlMetadataDir();

    const fpath = join(etl_metadata_dir, "PDA_APP_STORE.json");

    await writeFileAsync(fpath, JSON.stringify(PDA_APP_STORE, null, 4));
  }

  async writeRitisExportRequestNetworkObjects(
    data: RitisExportRequestNetworkObjects
  ) {
    const etl_metadata_dir = getEtlMetadataDir();

    await Promise.all(
      Object.entries(data).map(async ([k, v]) => {
        const fpath = join(
          etl_metadata_dir,
          `ritis_export_request.${k}.json.gz`
        );

        const d = await gzipAsync(JSON.stringify(v), { level: 9 });
        await writeFileAsync(fpath, d);
      })
    );
  }
}
