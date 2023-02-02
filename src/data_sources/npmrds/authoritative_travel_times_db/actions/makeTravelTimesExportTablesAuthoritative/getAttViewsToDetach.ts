import { DateTime, Interval } from "luxon";
import _ from "lodash";

import {
  ParsedNpmrdsTravelTimesExportTableMetadata,
  EttViewsMetaSummary,
} from "./domain";

export function getEttIntervalsByViewId(ettViewsMeta: EttViewsMetaSummary) {
  const errorMessages: string[] = [];

  const ettIntervalsByViewId = ettViewsMeta.sortedByStateThenStartDate.reduce(
    (acc, viewId, i, arr) => {
      const { damaViewId, state, data_start_date, data_end_date } =
        ettViewsMeta.byViewId[viewId];

      const dateRangeStart = DateTime.fromISO(data_start_date).startOf("day");
      const dateRangeEnd = DateTime.fromISO(data_end_date)
        .plus({ days: 1 })
        .startOf("day");

      const interval = Interval.fromDateTimes(dateRangeStart, dateRangeEnd);

      // Make sure there is no overlap between the ETT Views.
      if (i > 0) {
        const prevViewId = arr[i - 1];
        const { state: prevState } = ettViewsMeta.byViewId[prevViewId];

        if (state === prevState) {
          const prevInterval = acc[prevViewId];

          const intxn = prevInterval.intersection(interval);

          if (intxn !== null) {
            errorMessages.push(
              `ETT views ${prevViewId} and ${damaViewId} for state ${state} overlap: ${intxn.toISODate()}.`
            );
          }
        }
      }

      acc[damaViewId] = interval;

      return acc;
    },
    {}
  );

  if (errorMessages.length) {
    throw new Error(`INVARIANT VIOLATIONS:\n\t*${errorMessages.join("\n\t*")}`);
  }

  return ettIntervalsByViewId;
}

export function handleHasAttViewIdsCase(
  attViewsMeta: null | EttViewsMetaSummary,
  ettViewsMeta: EttViewsMetaSummary
) {
  const ettIntervalsByViewId = getEttIntervalsByViewId(ettViewsMeta);

  const sortedEttDateRangeIntervalsByState =
    ettViewsMeta.sortedByStateThenStartDate.reduce((acc, ettViewId) => {
      const { state } = ettViewsMeta.byViewId[ettViewId];
      const interval = ettIntervalsByViewId[ettViewId];

      acc[state] = acc[state] || [];
      acc[state].push(interval);

      return acc;
    }, {});

  //  These ATTs overlap with ETTs, therefore we must
  //    * detach them from the NpmrdsAuthoritativeTravelTimesDb partition tree
  //    * remove them from the new NpmrdsAuthoritativeTravelTimesDb DamaView's view_dependencies
  const attViewIdsToDetachSet: Set<number> = new Set();

  // Need to collect the keeper ATTs.
  const sortedKeeperAttDateRangeIntervalsByState: Record<string, Interval[]> =
    {};

  for (const attViewId of attViewsMeta?.sortedByStateThenStartDate || []) {
    const { state, year, month, data_start_date, data_end_date } = <
      ParsedNpmrdsTravelTimesExportTableMetadata
    >attViewsMeta?.byViewId[attViewId];

    // Get the ATT's Interval
    const attDateRangeStart = DateTime.fromISO(data_start_date).startOf("day");

    const attDateRangeEnd = DateTime.fromISO(data_end_date)
      .plus({ days: 1 })
      .startOf("day");

    const attInterval = Interval.fromDateTimes(
      attDateRangeStart,
      attDateRangeEnd
    );

    // ETTs for this state/year/month
    const hasOverlappingEtts = ettViewsMeta.byMonthByYearByState[state]?.[
      year
    ]?.[month]?.some((ettId) =>
      attInterval.overlaps(ettIntervalsByViewId[ettId])
    );

    if (hasOverlappingEtts) {
      // ETTs overlap the old ATT. Need to detach the ATT before attaching the ETTs.
      console.log("\n==> HAS OVERLAPPING ETTs\n");

      attViewIdsToDetachSet.add(attViewId);
    } else {
      // No ETTs overlap the old ATT. We keep the ATT.
      sortedKeeperAttDateRangeIntervalsByState[state] =
        sortedKeeperAttDateRangeIntervalsByState[state] || [];

      sortedKeeperAttDateRangeIntervalsByState[state].push(attInterval);
    }
  }

  const states = _.uniq([
    ...Object.keys(sortedEttDateRangeIntervalsByState),
    ...Object.keys(sortedKeeperAttDateRangeIntervalsByState),
  ]).sort();

  const sortedNewAttDateRangeIntervalsByState: Record<string, Interval[]> = {};

  // Need to merge the ETTs and keeper ATTs
  for (const state of states) {
    const newAttIntervals = (sortedNewAttDateRangeIntervalsByState[state] = <
      Interval[]
    >[]);

    const attIntervals = sortedKeeperAttDateRangeIntervalsByState[state] || [];
    const ettIntervals = sortedEttDateRangeIntervalsByState[state] || [];

    let attCursor = 0;
    let ettCursor = 0;

    while (attCursor < attIntervals.length || ettCursor < ettIntervals.length) {
      console.log("attCursor:", attCursor, "|", "ettCursor:", ettCursor);

      const attInterval = attIntervals[attCursor];
      const ettInterval = ettIntervals[ettCursor];

      if (!attInterval) {
        newAttIntervals.push(ettInterval);
        ++ettCursor;
        continue;
      }

      if (!ettInterval) {
        newAttIntervals.push(attInterval);
        ++attCursor;
        continue;
      }

      const { start: attStartDateTime } = attInterval;
      const { start: ettStartDateTime } = ettInterval;

      const attStartDate = attStartDateTime.toISODate();
      const ettStartDate = ettStartDateTime.toISODate();

      if (attStartDate < ettStartDate) {
        newAttIntervals.push(attInterval);
        ++attCursor;
        continue;
      } else {
        newAttIntervals.push(ettInterval);
        ++ettCursor;
        continue;
      }
    }
  }

  console.log(
    JSON.stringify(
      {
        sortedKeeperAttDateRangeIntervalsByState,
        sortedEttDateRangeIntervalsByState,
        sortedNewAttDateRangeIntervalsByState,
      },
      null,
      4
    )
  );

  return {
    sortedNewAttDateRangeIntervalsByState,
    attViewIdsToDetach: [...attViewIdsToDetachSet],
  };
}

export function handleNoAttViewIdsCase(ettViewsMeta: EttViewsMetaSummary) {
  const ettIntervalsByViewId = getEttIntervalsByViewId(ettViewsMeta);
  const sortedNewAttDateRangeIntervalsByState: Record<string, Interval[]> = {};

  for (const state of Object.keys(ettViewsMeta.byMonthByYearByState).sort()) {
    sortedNewAttDateRangeIntervalsByState[state] = [];

    const years = Object.keys(ettViewsMeta.byMonthByYearByState[state])
      .map((yr) => +yr)
      .sort((a, b) => a - b);

    for (const year of years) {
      const months = Object.keys(ettViewsMeta.byMonthByYearByState[state][year])
        .map((mo) => +mo)
        .sort((a, b) => a - b);

      for (const month of months) {
        const ettIntervals = ettViewsMeta.byMonthByYearByState[state][year][
          month
        ].map((ettViewId) => ettIntervalsByViewId[ettViewId]);

        sortedNewAttDateRangeIntervalsByState[state].push(...ettIntervals);
      }
    }
  }

  return {
    sortedNewAttDateRangeIntervalsByState,
    attViewIdsToDetach: <number[]>[],
  };
}

export function validateNoAttGaps(
  attViewsMeta: null | EttViewsMetaSummary,
  sortedNewAttDateRangeIntervalsByState: Record<string, Interval[]>
) {
  const errorMessages: string[] = [];

  const newAttIntervalUnionsByState: Record<string, Interval> = {};

  // TEST: No gaps within the new ATTs
  for (const state of Object.keys(
    sortedNewAttDateRangeIntervalsByState
  ).sort()) {
    const intervals = sortedNewAttDateRangeIntervalsByState[state];

    const union = <Interval>(
      _.first(intervals)?.union(<Interval>_.last(intervals))
    );

    newAttIntervalUnionsByState[state] = union;

    console.log("=".repeat(10), state, "=".repeat(10));
    for (const interval of intervals) {
      console.log(interval.toISO());
    }
    console.log("=".repeat(24));

    let [prevInterval] = intervals;

    for (const curInterval of intervals.slice(1)) {
      if (!prevInterval.abutsStart(curInterval)) {
        console.log("==> prevInterval", prevInterval.toISO());
        console.log("==> curInterval", curInterval.toISO());

        const [difference] = prevInterval.difference(curInterval);

        errorMessages.push(
          `For state ${state} there is an ATT gap ${difference.toISODate()}`
        );
      }

      prevInterval = curInterval;
    }
  }

  if (attViewsMeta !== null) {
    const oldAttIntervalUnionsByState: Record<string, Interval> = Object.keys(
      attViewsMeta.byMonthByYearByState
    ).reduce((acc, state) => {
      const firstAttViewId = <number>(
        _.first(attViewsMeta.sortedByStateThenStartDate)
      );
      const lastAttViewId = <number>(
        _.last(attViewsMeta.sortedByStateThenStartDate)
      );

      const { data_start_date } = attViewsMeta.byViewId[firstAttViewId];
      const { data_end_date } = attViewsMeta.byViewId[lastAttViewId];

      const startDateTime = DateTime.fromISO(data_start_date);
      const endDateTime = DateTime.fromISO(data_end_date)
        .plus({ days: 1 })
        .startOf("day");

      const interval = Interval.fromDateTimes(startDateTime, endDateTime);

      acc[state] = interval;

      return acc;
    }, {});

    console.log(
      JSON.stringify(
        { attViewsMeta, sortedNewAttDateRangeIntervalsByState },
        null,
        4
      )
    );

    const states = _.uniq([
      ...Object.keys(oldAttIntervalUnionsByState),
      ...Object.keys(newAttIntervalUnionsByState),
    ]).sort();

    console.log(states);

    for (const state of states) {
      const oldInterval = oldAttIntervalUnionsByState[state];
      const newInterval = newAttIntervalUnionsByState[state];

      if (oldInterval && !newInterval.engulfs(oldInterval)) {
        const oldStr = oldInterval.toISODate();
        const newStr = newInterval.toISODate();

        errorMessages.push(
          `For state ${state} the old interval was ${oldStr} but it is now ${newStr}`
        );
      }
    }
  }

  if (errorMessages.length) {
    throw new Error(`INVARIANT VIOLATIONS:\n\t*${errorMessages.join("\n\t*")}`);
  }
}

export default function getAttViewsToDetach(
  attViewsMeta: null | EttViewsMetaSummary,
  ettViewsMeta: EttViewsMetaSummary
) {
  const hasAttViewIds = !!attViewsMeta?.sortedByStateThenStartDate?.length;

  const { sortedNewAttDateRangeIntervalsByState, attViewIdsToDetach } =
    hasAttViewIds
      ? handleHasAttViewIdsCase(attViewsMeta, ettViewsMeta)
      : handleNoAttViewIdsCase(ettViewsMeta);

  console.log(JSON.stringify({ attViewIdsToDetach }, null, 4));

  validateNoAttGaps(attViewsMeta, sortedNewAttDateRangeIntervalsByState);

  const attViewsToDetach = attViewsMeta
    ? attViewIdsToDetach.map((attViewId) => attViewsMeta.byViewId[attViewId])
    : [];

  // FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME
  // Need to guarantee that the new ATTs cover at least as much time as the old.
  //  EG: 20220101-20220131 cannot be replaced with 20220101-20220116
  return attViewsToDetach;
}
