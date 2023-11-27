/* eslint-disable max-len */

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
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(2) > div.DateRangeSelector > div > div.range-list-wrapper > div > div.DateRange > div.date-range-wrapper.lower-date > div.datepicker-input-wrapper > div > div.react-datepicker-wrapper > div > input";

export const lowerDateDatePicker =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(2) > div.DateRangeSelector > div > div.range-list-wrapper > div > div.DateRange > div.date-range-wrapper.lower-date > div.datepicker-input-wrapper > div > div.react-datepicker-popper > div";

export const lowerDateYearSelector =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(2) > div.DateRangeSelector > div > div.range-list-wrapper > div > div.DateRange > div.date-range-wrapper.lower-date > div.datepicker-input-wrapper > div > div.react-datepicker-popper > div > div.react-datepicker__month-container > div.react-datepicker__header > div.react-datepicker__header__dropdown.react-datepicker__header__dropdown--select > div.react-datepicker__year-dropdown-container.react-datepicker__year-dropdown-container--select > select";

export const lowerDateMonthSelector =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(2) > div.DateRangeSelector > div > div.range-list-wrapper > div > div.DateRange > div.date-range-wrapper.lower-date > div.datepicker-input-wrapper > div > div.react-datepicker-popper > div > div.react-datepicker__month-container > div.react-datepicker__header > div.react-datepicker__header__dropdown.react-datepicker__header__dropdown--select > div.react-datepicker__month-dropdown-container.react-datepicker__month-dropdown-container--select > select";

export const lowerDateDaysMultiSelector =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(2) > div.DateRangeSelector > div > div.range-list-wrapper > div > div.DateRange > div.date-range-wrapper.lower-date > div.datepicker-input-wrapper > div > div.react-datepicker-popper > div > div.react-datepicker__month-container * div.react-datepicker__day";

export const upperDateOpenCalendarTarget =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(2) > div.DateRangeSelector > div > div.range-list-wrapper > div > div.DateRange > div.date-range-wrapper.upper-date > div.datepicker-input-wrapper > div > div.react-datepicker-wrapper > div > input";

export const upperDateDatePicker =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(2) > div.DateRangeSelector > div > div.range-list-wrapper > div > div.DateRange > div.date-range-wrapper.upper-date > div.datepicker-input-wrapper > div > div.react-datepicker-popper > div";

export const upperDateYearSelector =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(2) > div.DateRangeSelector > div > div.range-list-wrapper > div > div.DateRange > div.date-range-wrapper.upper-date > div.datepicker-input-wrapper > div > div.react-datepicker-popper > div > div.react-datepicker__month-container > div.react-datepicker__header > div.react-datepicker__header__dropdown.react-datepicker__header__dropdown--select > div.react-datepicker__year-dropdown-container.react-datepicker__year-dropdown-container--select > select";

export const upperDateMonthSelector =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(2) > div.DateRangeSelector > div > div.range-list-wrapper > div > div.DateRange > div.date-range-wrapper.upper-date > div.datepicker-input-wrapper > div > div.react-datepicker-popper > div > div.react-datepicker__month-container > div.react-datepicker__header > div.react-datepicker__header__dropdown.react-datepicker__header__dropdown--select > div.react-datepicker__month-dropdown-container.react-datepicker__month-dropdown-container--select > select";

export const upperDateDaysMultiSelector =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(2) > div.DateRangeSelector > div > div.range-list-wrapper > div > div.DateRange > div.date-range-wrapper.upper-date > div.datepicker-input-wrapper > div > div.react-datepicker-popper > div > div.react-datepicker__month-container * div.react-datepicker__day";

export const downloadNameField =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(9) > div.TitleInput > input[type=text]";

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

export const tmcsSearchRegionTab =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(1) > div:nth-child(2) > div > div.TabNavigator > div.Tabs > div:nth-child(2) > div > div > span";

export const regionSelectorsList =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(1) > div:nth-child(2) > div > div.TabNavigator > div.ContentContainer > div.active-container > div > div > div > div > div > div:nth-child(1) > div > div.input-wrapper > div > div";

// The regionSelectorsList ONLY when it is opened
export const openedRegionsSelectorsList =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(1) > div:nth-child(2) > div > div.TabNavigator > div.ContentContainer > div.active-container > div > div > div > div > div > div:nth-child(1) > div > div.input-wrapper > div > div.toggle-button.active";

export const allRegionsCheckBox =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(1) > div:nth-child(2) > div > div.TabNavigator > div.ContentContainer > div.active-container > div > div > div > div > div > div:nth-child(1) > div > div.input-wrapper > div > div:nth-child(2) > div.parent-select-all > label > input[type=checkbox]";

export const allRegionsClickTarget =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(1) > div:nth-child(2) > div > div.TabNavigator > div.ContentContainer > div.active-container > div > div > div > div > div > div:nth-child(1) > div > div.input-wrapper > div > div:nth-child(2) > div.parent-select-all > label > span";

export const stateRegionsListOptions =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(1) > div:nth-child(2) > div > div.TabNavigator > div.ContentContainer > div.active-container > div > div > div > div > div > div:nth-child(1) > div > div.input-wrapper > div * .dropdown-option";

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

// export const addRegionSelectedRoads =
// "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(1) > div:nth-child(2) > div > div.TabNavigator > div.ContentContainer > div.active-container > div > div > div > div > div > div.footer > div > div.Text";

export const addRegionSelectedRoadsButton =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(1) > div:nth-child(2) > div > div.TabNavigator > div.ContentContainer > div.active-container > div > div > div > div > div > div.footer > div";

export const removeAllSelectedRoads =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(1) > div:nth-child(2) > div > div.SelectedRoadList > div > div.list-header > a > span";

export const confirmRemoveAllSelectedRoads =
  "body > main > div > div.global-notifications > div > div.popup-container > div.footer > div > div > div:nth-child(1) > button";

// export function getRegionSelectorListItemByIndex(i: number) {
// return `body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(1) > div:nth-child(2) > div > div.TabNavigator > div.ContentContainer > div.active-container > div > div > div > div > div > div:nth-child(1) > div > div.input-wrapper > div > div:nth-child(2) > div.options-container.offset-container > div:nth-child(${i})`;
// }

export const showSegementIdsButton =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(1) > div:nth-child(2) > div > div.SelectedRoadList > div > div.selected-road-buttons > div.show-segments-button.IconButton";

export const showSegmentIdsPopupTextArea =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(1) > div:nth-child(2) > div > div.SelectedRoadList > div > div.selected-road-buttons > div.SegmentIdsPopup > div > div.popup-container > div.body > textarea";

export const closeShowSegmentIdsPopup =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(1) > div:nth-child(2) > div > div.SelectedRoadList > div > div.selected-road-buttons > div:nth-child(3)";

export const selectDayOfWeekButtons = {
  sunday:
    "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(3) > div.DayOfWeekSelector > div.days > div:nth-child(1)",

  monday:
    "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(3) > div.DayOfWeekSelector > div.days > div:nth-child(2)",

  tuesday:
    "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(3) > div.DayOfWeekSelector > div.days > div:nth-child(3)",

  wednesday:
    "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(3) > div.DayOfWeekSelector > div.days > div:nth-child(4)",

  thursday:
    "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(3) > div.DayOfWeekSelector > div.days > div:nth-child(5)",

  friday:
    "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(3) > div.DayOfWeekSelector > div.days > div:nth-child(6)",

  saturday:
    "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(3) > div.DayOfWeekSelector > div.days > div:nth-child(7)",
};

export const startTime =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(4) > div.TimeOfDaySelector > div > div.range-list-wrapper > div > div.TimeRange > div > div.time-range-wrapper.lower-time > div.timepicker-input-wrapper.valid > input[type=text]";

export const startTimeMeridiem =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(4) > div.TimeOfDaySelector > div > div.range-list-wrapper > div > div.TimeRange > div > div.time-range-wrapper.lower-time > div.meridiem-wrapper > div.select-wrapper > select";

export const amStartTimeMeridiemOption =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(4) > div.TimeOfDaySelector > div > div.range-list-wrapper > div > div.TimeRange > div > div.time-range-wrapper.lower-time > div.meridiem-wrapper > div.select-wrapper > select > option:nth-child(1)";

export const endTime =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(4) > div.TimeOfDaySelector > div > div.range-list-wrapper > div > div.TimeRange > div > div.time-range-wrapper.upper-time > div.timepicker-input-wrapper.valid > input[type=text]";

export const endTimeMeridiem =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(4) > div.TimeOfDaySelector > div > div.range-list-wrapper > div > div.TimeRange > div > div.time-range-wrapper.upper-time > div.meridiem-wrapper > div.select-wrapper > select";

export const dataSources = {
  PASS: {
    root: "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(4) > div.TimeOfDaySelector > div > div.range-list-wrapper > div > div.TimeRange > div > div.time-range-wrapper.upper-time > div.timepicker-input-wrapper.valid > input[type=text]",
  },
};

export const passengerVehiclesDataSourceRootCheckbox =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(5) > div.DatasourceSelector > ul:nth-child(1) > li.CheckBox.datasource-checkbox.start-of-data-source-group > span.CheckBoxGroup-checkbox";

export const passengerVehiclesDataSourceClickTarget =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(5) > div.DatasourceSelector > ul:nth-child(1) > li.CheckBox.datasource-checkbox.start-of-data-source-group > span.CheckBoxGroup-node-content-wrapper";

export const allVehiclesDataSourceRootCheckbox =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(5) > div.DatasourceSelector > ul:nth-child(2) > li > span.CheckBoxGroup-checkbox";

export const allVehiclesDataSourceClickTarget =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(5) > div.DatasourceSelector > ul:nth-child(2) > li > span.CheckBoxGroup-node-content-wrapper";

export const trucksDataSourceRootCheckbox =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(5) > div.DatasourceSelector > ul:nth-child(3) > li > span.CheckBoxGroup-checkbox";

export const trucksDataSourceClickTarget =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(5) > div.DatasourceSelector > ul:nth-child(3) > li > span.CheckBoxGroup-node-content-wrapper";

export const timeUnitsSecondsRadioButton =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(6) > div.TravelTimeUnitsSelector > div > div:nth-child(1) > label > input[type=radio]";

export const timeUnitSecondsClickTarget =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(6) > div.TravelTimeUnitsSelector > div > div:nth-child(1) > label";

export const includeNullRecordsCheckbox =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(7) > div.AddNullRecordsCheckbox > label > input[type=checkbox]";

export const includeNullRecordsClickTarget =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(7) > div.AddNullRecordsCheckbox > label";

export const dontAverageTravelTimesRadioButton =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(8) > div.AveragingWindowSizeSelector > div > div:nth-child(1) > label > input[type=radio]";

export const dontAverageTravelTimesClickTarget =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(8) > div.AveragingWindowSizeSelector > div > div:nth-child(1) > label";

export const sendNotificationsCheckBox =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(10) > div.EmailNotificationCheckbox > label > input[type=checkbox]";

export const sendNotificationsClickTarget =
  "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(10) > div.EmailNotificationCheckbox > label > span";

export const formSubmittedModalOkButton =
  "body > main > div > div.global-notifications > div > div.popup-container > div.footer > div > div > div > button";

// body > main > div > div.global-notifications > div > div.popup-container > div.footer > div > div > div > button

export const pleaseChooseAtLeastOneStateWarningText =
  "body > main > div > div.global-notifications > div > div.popup-container > div.body";

export const pleaseChooseAtLeastOneStateAlertButton =
  "body > main > div > div.global-notifications > div > div.popup-container > div.footer > div > div > div > button";
