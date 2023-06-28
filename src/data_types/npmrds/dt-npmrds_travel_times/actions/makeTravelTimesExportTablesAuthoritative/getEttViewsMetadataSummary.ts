import dedent from "dedent";
import { DateTime } from "luxon";

import dama_db from "data_manager/dama_db";

import {
  ParsedNpmrdsTravelTimesExportTableMetadata,
  EttViewsMetaSummary,
} from "./domain";

import { NpmrdsDataSources } from "../../../domain";

const { NpmrdsTravelTimesImports } = NpmrdsDataSources;

export function parseNpmrdsTravelTimesExportTableMetadata(
  ettViewMeta: any
): ParsedNpmrdsTravelTimesExportTableMetadata {
  const {
    view_id: damaViewId,
    table_schema,
    table_name,
    last_updated,
    metadata: { state, is_expanded, start_date, end_date },
  } = ettViewMeta;

  const lastUpdated = (<Date>last_updated).toISOString();

  const startDateTime = DateTime.fromISO(start_date);

  const {
    year: dataStartYear,
    month: dataStartMonth,
    day: dataStartDay,
  } = startDateTime;

  const isStartOfMonth = dataStartDay === 1;

  const isStartOfWeek =
    isStartOfMonth ||
    startDateTime.get("day") === startDateTime.startOf("week").get("day");

  const endDateTime = DateTime.fromISO(end_date);

  const isEndOfMonth =
    endDateTime.get("day") === endDateTime.endOf("month").get("day");

  const isEndOfWeek =
    isEndOfMonth ||
    endDateTime.get("day") === endDateTime.endOf("week").get("day");

  const is_complete_month = isStartOfMonth && isEndOfMonth;
  const is_complete_week = !is_complete_month && isStartOfWeek && isEndOfWeek;

  return {
    damaViewId,

    table_schema,
    table_name,

    lastUpdated,

    state,
    year: dataStartYear,
    month: dataStartMonth,

    is_complete_week,
    is_complete_month,

    is_expanded: !!is_expanded,

    start_date,
    end_date,
  };
}

export default async function getEttViewsMetadataSummary(
  damaViewIds: number[]
): Promise<EttViewsMetaSummary | null> {
  const sql = dedent(`
    -- SELECT FOR SHARE
    SELECT
        a.name AS source_name,
        b.*
      FROM data_manager.sources AS a
        INNER JOIN data_manager.views AS b
          USING ( source_id )
      WHERE ( b.view_id = ANY($1) )
  `);

  const { rows } = await dama_db.query({
    text: sql,
    values: [damaViewIds],
  });

  const nonEttViews = rows.filter(
    ({ source_name }) => source_name !== NpmrdsTravelTimesImports
  );

  if (nonEttViews.length) {
    const errList = nonEttViews
      .map(
        ({ source_name, view_id }) =>
          `DamaViewId ${view_id} is for DamaSource ${source_name}`
      )
      .join("\n\t* ");

    const header = `INVARIANT${errList.length > 1 ? "S" : ""} BROKEN`;

    throw new Error(`${header}\n\t* ${errList}`);
  }

  if (rows.length === 0) {
    return null;
  }

  const ettViewsMeta = rows.map((ettViewMeta) =>
    parseNpmrdsTravelTimesExportTableMetadata(ettViewMeta)
  );

  const { byViewId, dataDateRange, lastUpdated } = ettViewsMeta.reduce(
    (acc, row) => {
      const { damaViewId, lastUpdated, start_date, end_date } = row;

      acc.byViewId[damaViewId] = row;

      if (acc.lastUpdated < lastUpdated) {
        acc.lastUpdated = lastUpdated;
      }

      if (!acc.dataDateRange[0] || acc.dataDateRange[0] > start_date) {
        acc.dataDateRange[0] = start_date;
      }

      if (!acc.dataDateRange[1] || acc.dataDateRange[1] < end_date) {
        acc.dataDateRange[1] = end_date;
      }

      return acc;
    },
    {
      byViewId: {},
      dataDateRange: <[string, string]>[
        ettViewsMeta[0].start_date,
        ettViewsMeta[0].end_date,
      ],
      lastUpdated: ettViewsMeta[0].lastUpdated,
    }
  );

  const sortedByStateThenStartDate = ettViewsMeta
    .sort((rowA, rowB) => {
      const { state: stateA, start_date: startDateA } = rowA;
      const { state: stateB, start_date: startDateB } = rowB;

      return (
        stateA.localeCompare(stateB) || startDateA.localeCompare(startDateB)
      );
    })
    .map(({ damaViewId }) => damaViewId);

  const byMonthByYearByState = sortedByStateThenStartDate.reduce(
    (acc, viewId) => {
      const viewMeta = byViewId[viewId];

      const { damaViewId, state, year, month } = viewMeta;

      acc[state] = acc[state] || {};
      acc[state][year] = acc[state][year] || {};
      acc[state][year][month] = acc[state][year][month] || [];
      acc[state][year][month].push(damaViewId); // Note: sorted chronologically

      return acc;
    },
    {}
  );

  return {
    byViewId,
    sortedByStateThenStartDate,
    byMonthByYearByState,
    dataDateRange,
    lastUpdated,
  };
}
