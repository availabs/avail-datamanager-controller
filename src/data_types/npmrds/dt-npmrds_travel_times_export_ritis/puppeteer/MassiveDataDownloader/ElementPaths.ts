/* eslint-disable max-len */

import { RitisExportNpmrdsDataSource } from "../../domain";

export const pageTitle =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > div > div.header-content > div.title";

export const queryFormSelector = "body > main > div > div.QueryFormContainer";

export const tmcDataSourceDropDownDiv =
  "#SimpleDropDown-TMCDataSourceDropDown > div";

export const npmrdsInrixMapYearDropDown = "#react-select-2--value-item";

export const npmrdsInrixMapYearDropDownList = "#react-select-2--list";
export const npmrdsInrixMapYearDropDownOptionsContainer =
  "#react-select-2--list > div";

export const getNpmrdsInrixMapYearDropDownOptionByIndex = (i: number) =>
  // Note: Caller must to increment by 1 since selector uses 1 div above DropDownOptionsContainer,
  `#react-select-2--list > div > div:nth-child(${i})`;

export const lowerDateOpenCalendarTarget =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(3) > div.DateRangeSelector > div > div.range-list-wrapper > div > div.DateRange > div.date-range-wrapper.lower-date > div.datepicker-input-wrapper > div.react-datepicker-wrapper > div > input";

export const lowerDateDatePicker =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(3) > div.DateRangeSelector > div > div.range-list-wrapper > div > div.DateRange > div.date-range-wrapper.lower-date > div.datepicker-input-wrapper > div > div.react-datepicker-popper > div";

export const lowerDateYearSelector =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(3) > div.DateRangeSelector > div > div.range-list-wrapper > div > div.DateRange > div.date-range-wrapper.lower-date > div.datepicker-input-wrapper > div.react-datepicker__tab-loop > div.react-datepicker-popper > div > div > div.react-datepicker__month-container > div.react-datepicker__header > div.react-datepicker__header__dropdown.react-datepicker__header__dropdown--select > div.react-datepicker__year-dropdown-container.react-datepicker__year-dropdown-container--select > select";

export const lowerDateMonthSelector =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(3) > div.DateRangeSelector > div > div.range-list-wrapper > div > div.DateRange > div.date-range-wrapper.lower-date > div.datepicker-input-wrapper > div.react-datepicker__tab-loop > div.react-datepicker-popper > div > div > div.react-datepicker__month-container > div.react-datepicker__header > div.react-datepicker__header__dropdown.react-datepicker__header__dropdown--select > div.react-datepicker__month-dropdown-container.react-datepicker__month-dropdown-container--select > select";

export const lowerDateDaysMultiSelector =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(3) > div.DateRangeSelector > div > div.range-list-wrapper > div > div.DateRange > div.date-range-wrapper.lower-date > div.datepicker-input-wrapper > div.react-datepicker__tab-loop > div.react-datepicker-popper > div > div > div.react-datepicker__month-container > div.react-datepicker__month > * > div.react-datepicker__day";

export const upperDateOpenCalendarTarget =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(3) > div.DateRangeSelector > div > div.range-list-wrapper > div > div.DateRange > div.date-range-wrapper.upper-date > div.datepicker-input-wrapper > div.react-datepicker-wrapper > div > input";

export const upperDateDatePicker =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(3) > div.DateRangeSelector > div > div.range-list-wrapper > div > div.DateRange > div.date-range-wrapper.upper-date > div.datepicker-input-wrapper > div > div.react-datepicker-popper > div";

export const upperDateYearSelector =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(3) > div.DateRangeSelector > div > div.range-list-wrapper > div > div.DateRange > div.date-range-wrapper.upper-date > div.datepicker-input-wrapper > div.react-datepicker__tab-loop > div.react-datepicker-popper > div > div > div.react-datepicker__month-container > div.react-datepicker__header > div.react-datepicker__header__dropdown.react-datepicker__header__dropdown--select > div.react-datepicker__year-dropdown-container.react-datepicker__year-dropdown-container--select > select";

export const upperDateMonthSelector =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(3) > div.DateRangeSelector > div > div.range-list-wrapper > div > div.DateRange > div.date-range-wrapper.upper-date > div.datepicker-input-wrapper > div.react-datepicker__tab-loop > div.react-datepicker-popper > div > div > div.react-datepicker__month-container > div.react-datepicker__header > div.react-datepicker__header__dropdown.react-datepicker__header__dropdown--select > div.react-datepicker__month-dropdown-container.react-datepicker__month-dropdown-container--select > select";

export const upperDateDaysMultiSelector =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(3) > div.DateRangeSelector > div > div.range-list-wrapper > div > div.DateRange > div.date-range-wrapper.upper-date > div.datepicker-input-wrapper > div.react-datepicker__tab-loop > div.react-datepicker-popper > div > div > div.react-datepicker__month-container > div.react-datepicker__month > * > div.react-datepicker__day";

export const downloadNameField =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(11) > div.TitleInput > input[type=text]";

export const submitButton =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > button";

export const expandedMapCheckBox =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(1) > div:nth-child(2) > div > div:nth-child(2) > label > input[type=checkbox]";

export const expandedMapClickTarget =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(1) > div:nth-child(2) > div > div:nth-child(2) > label > span";

// NOTE: may select multiple elements
export const selectedRoadsListItem = ".SelectedRoadListItem";

export const selectedRoadsInfoSpan =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(1) > div:nth-child(2) > div > div.SelectedRoadList > div > div.selected-road-list-container > div > div > div.title-wrapper > span > span";

// We use this as a proxy to make sure the Select roads section is ready.
export const tmcSearchInRegionsBox =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(2) > div:nth-child(2) > div > div.TabNavigator > div.ContentContainer > div > div > div > div > div > div.SimpleRoadSearch > div > div > input";

export const tmcsSearchRegionTab =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(2) > div:nth-child(2) > div > div.TabNavigator > div.Tabs > div:nth-child(2) > div > div > span";

export const regionSelectorsList =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(2) > div:nth-child(2) > div > div.TabNavigator > div.ContentContainer > div.active-container > div > div > div > div > div > div:nth-child(1) > div > div.input-wrapper > div > div";
//  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(2) > div:nth-child(2) > div > div.TabNavigator > div.ContentContainer > div > div > div > div > div > div > div:nth-child(1) > div > div.input-wrapper > div > div"

// The regionSelectorsList ONLY when it is opened
export const openedRegionsSelectorsList =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(2) > div:nth-child(2) > div > div.TabNavigator > div.ContentContainer > div.active-container > div > div > div > div > div > div:nth-child(1) > div > div.input-wrapper > div > div.toggle-button.active";

export const allRegionsCheckBox =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(2) > div:nth-child(2) > div > div.TabNavigator > div.ContentContainer > div.active-container > div > div > div > div > div > div:nth-child(1) > div > div.input-wrapper > div > div:nth-child(2) > div.parent-select-all > label > input[type=checkbox]";

export const allRegionsClickTarget =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(2) > div:nth-child(2) > div > div.TabNavigator > div.ContentContainer > div.active-container > div > div > div > div > div > div:nth-child(1) > div > div.input-wrapper > div > div:nth-child(2) > div.parent-select-all > label > span";

export const stateRegionsListOptions =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(2) > div:nth-child(2) > div > div.TabNavigator > div.ContentContainer > div.active-container > div > div > div > div > div > div:nth-child(1) > div > div.input-wrapper > div * .dropdown-option";

export const gatheringSegmentsSpinner =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(1) > div:nth-child(2) > div > div.TabNavigator > div.ContentContainer > div.active-container > div > div > div > div > div > div.footer > div.search-feedback > span";

export const tmcsSegmentCodesTab =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(1) > div:nth-child(2) > div > div.TabNavigator > div.Tabs > div:nth-child(3) > div > div";

export const tmcsSegmentCodesTextArea =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(1) > div:nth-child(2) > div > div.TabNavigator > div.ContentContainer > div.active-container > div > div > div > div > textarea";

export const autoRefreshMapClickTarget =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(1) > div:nth-child(2) > div > div:nth-child(2) > label > span";

export const autoRefreshMapCheckBox =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(1) > div:nth-child(2) > div > div:nth-child(2) > label > input[type=checkbox]";

export const addTmcSegmentCodesButton =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(1) > div:nth-child(2) > div > div.TabNavigator > div.ContentContainer > div.active-container > div > div > div > div > div.footer > div";

export const addTmcSegmentCodesSpinner =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(1) > div:nth-child(2) > div > div.SelectedRoadList > div > div.selected-road-list-container > div > div > div.title-wrapper > span > img";

export const addRegionSelectedRoadsButton =
  // "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(2) > div:nth-child(2) > div > div.TabNavigator > div.ContentContainer > div.active-container > div > div > div > div > div > div.footer > div";
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(2) > div:nth-child(2) > div > div.TabNavigator > div.ContentContainer > div > div > div > div > div > div > div.footer > div > div";

export const removeAllSelectedRoads =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(1) > div:nth-child(2) > div > div.SelectedRoadList > div > div.list-header > a > span";

export const confirmRemoveAllSelectedRoads =
  "body > main > div > div.global-notifications > div > div.popup-container > div.footer > div > div > div:nth-child(1) > button";

export const showSegementIdsButton =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(2) > div:nth-child(2) > div > div.SelectedRoadList > div > div.selected-road-buttons > div.show-segments-button.IconButton";

export const showSegmentIdsPopupTextArea =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(2) > div:nth-child(2) > div > div.SelectedRoadList > div > div.selected-road-buttons > div.SegmentIdsPopup > div > div.popup-container > div.body > textarea";

export const closeShowSegmentIdsPopup =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(2) > div:nth-child(2) > div > div.SelectedRoadList > div > div.selected-road-buttons > div:nth-child(3)";

export const selectDayOfWeekButtons = {
  sunday:
    "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(4) > div.DayOfWeekSelector > div.days > div:nth-child(1)",

  monday:
    "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(4) > div.DayOfWeekSelector > div.days > div:nth-child(2)",

  tuesday:
    "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(4) > div.DayOfWeekSelector > div.days > div:nth-child(3)",

  wednesday:
    "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(4) > div.DayOfWeekSelector > div.days > div:nth-child(4)",

  thursday:
    "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(4) > div.DayOfWeekSelector > div.days > div:nth-child(5)",

  friday:
    "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(4) > div.DayOfWeekSelector > div.days > div:nth-child(6)",

  saturday:
    "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(4) > div.DayOfWeekSelector > div.days > div:nth-child(7)",
};

export const startTime =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(5) > div.TimeOfDaySelector > div > div.range-list-wrapper > div > div.TimeRange > div > div.time-range-wrapper.lower-time > div.timepicker-input-wrapper.valid > input[type=text]";

export const startTimeMeridiem =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(5) > div.TimeOfDaySelector > div > div.range-list-wrapper > div > div.TimeRange > div > div.time-range-wrapper.lower-time > div.meridiem-wrapper > div.select-wrapper > select";

export const amStartTimeMeridiemOption =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(5) > div.TimeOfDaySelector > div > div.range-list-wrapper > div > div.TimeRange > div > div.time-range-wrapper.lower-time > div.meridiem-wrapper > div.select-wrapper > select > option:nth-child(1)";

export const endTime =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(5) > div.TimeOfDaySelector > div > div.range-list-wrapper > div > div.TimeRange > div > div.time-range-wrapper.upper-time > div.timepicker-input-wrapper.valid > input[type=text]";

export const endTimeMeridiem =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(5) > div.TimeOfDaySelector > div > div.range-list-wrapper > div > div.TimeRange > div > div.time-range-wrapper.upper-time > div.meridiem-wrapper > div.select-wrapper > select";

export const dataSources = {
  PASS: {
    root: "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(4) > div.TimeOfDaySelector > div > div.range-list-wrapper > div > div.TimeRange > div > div.time-range-wrapper.upper-time > div.timepicker-input-wrapper.valid > input[type=text]",
  },
};

export enum ExportDataSourceColumns {
  Speed = "Speed",
  HistoricalAverageSpeed = "HistoricalAverageSpeed",
  ReferenceSpeed = "ReferenceSpeed",
  TravelTime = "TravelTime",
  DataDensity = "DataDensity",
  AADT = "AADT",
}

// The order that the column options appear in the drop down checklist.
export const ExportDataSourceColumnsListIndex = {
  [ExportDataSourceColumns.Speed]: 0,
  [ExportDataSourceColumns.HistoricalAverageSpeed]: 1,
  [ExportDataSourceColumns.ReferenceSpeed]: 2,
  [ExportDataSourceColumns.TravelTime]: 3,
  [ExportDataSourceColumns.DataDensity]: 4,
  [ExportDataSourceColumns.AADT]: 5,
};

const getDataFieldSelectorPathPrefix = (
  dataSource: RitisExportNpmrdsDataSource
) => {
  const dataSourceToIndex = {
    [RitisExportNpmrdsDataSource.PASSENGER_VEHICLES]: 1,
    [RitisExportNpmrdsDataSource.ALL_VEHICLES]: 2,
    [RitisExportNpmrdsDataSource.TRUCKS]: 3,
  };

  const idx = dataSourceToIndex[dataSource];
  console.log("dataSource=", dataSource, ", idx=", idx);

  return `body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(6) > div.DatasourceFieldsSelector > ul:nth-child(${idx}) > li`;
};

const getDataSourceClickTarget = (dataSource: RitisExportNpmrdsDataSource) => {
  return `${getDataFieldSelectorPathPrefix(dataSource)} > span > span`;
};

const getDataSourceColumnsList = (dataSource: RitisExportNpmrdsDataSource) => {
  return `${getDataFieldSelectorPathPrefix(
    dataSource
  )} > ul > li > span.CheckBoxGroup-checkbox`;
};

export const getDataSourceColumnCheckboxes = (
  dataSource: RitisExportNpmrdsDataSource
) => {
  const prefix = getDataFieldSelectorPathPrefix(dataSource);

  const checkBoxPaths = Object.keys(ExportDataSourceColumnsListIndex).reduce(
    (acc, k) => {
      const idx = ExportDataSourceColumnsListIndex[k];
      acc[k] = `${prefix} > ul > li:nth-child(${
        idx + 1
      }) > span.CheckBoxGroup-node-content-wrapper.CheckBoxGroup-node-content-wrapper-normal`;

      return acc;
    },
    {}
  );

  return checkBoxPaths;
};

export const dataSourceCheckboxPaths = [
  RitisExportNpmrdsDataSource.PASSENGER_VEHICLES,
  RitisExportNpmrdsDataSource.ALL_VEHICLES,
  RitisExportNpmrdsDataSource.TRUCKS,
].reduce((acc, dataSource) => {
  acc[dataSource] = {
    dataSourceClickTarget: getDataSourceClickTarget(dataSource),
    dataSourceColumnsList: getDataSourceColumnsList(dataSource),
    dataSourceColumnCheckboxes: getDataSourceColumnCheckboxes(dataSource),
  };

  return acc;
}, {});

export const timeUnitsSecondsRadioButton =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(7) > div.TravelTimeUnitsSelector > div > div:nth-child(1) > label > input[type=radio]";

export const timeUnitSecondsClickTarget =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(7) > div.TravelTimeUnitsSelector > div > div:nth-child(1) > label";

export const includeNullRecordsCheckbox =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(9) > div.AddNullRecordsCheckbox > label > input[type=checkbox]";

export const includeNullRecordsClickTarget =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(9) > div.AddNullRecordsCheckbox > label";

export const dontAverageTravelTimesRadioButton =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(10) > div.AveragingWindowSizeSelector > div > div:nth-child(1) > label > input[type=radio]";

export const dontAverageTravelTimesClickTarget =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(10) > div.AveragingWindowSizeSelector > div > div:nth-child(1) > label";

export const sendNotificationsCheckBox =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(12) > div.EmailNotificationCheckbox > label > input[type=checkbox]";

export const sendNotificationsClickTarget =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(12) > div.EmailNotificationCheckbox > label > span";

export const formSubmittedModalOkButton =
  "body > main > div > div.global-notifications > div > div.popup-container > div.footer > div > div > div > button";

export const pleaseChooseAtLeastOneStateWarningText =
  "body > main > div > div.global-notifications > div > div.popup-container > div.body";

export const pleaseChooseAtLeastOneStateAlertButton =
  "body > main > div > div.global-notifications > div > div.popup-container > div.footer > div > div > div > button";
