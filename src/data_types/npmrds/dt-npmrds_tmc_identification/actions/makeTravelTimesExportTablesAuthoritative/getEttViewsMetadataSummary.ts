import dedent from "dedent";
import { DateTime } from "luxon";

import {
  ParsedNpmrdsTravelTimesExportTableMetadata,
  EttViewsMetaSummary,
  NodePgDbConnection,
} from "./domain";

import { NpmrdsDataSources } from "../../../domain";

const { NpmrdsTravelTimesImp } = NpmrdsDataSources;

export function parseNpmrdsTravelTimesExportTableMetadata(
  ettViewMeta: any
): ParsedNpmrdsTravelTimesExportTableMetadata {
  const {
    view_id: damaViewId,
    table_schema: tableSchema,
    table_name: tableName,
    last_updated,
    metadata: { state, is_expanded, data_start_date, data_end_date },
  } = ettViewMeta;

  const lastUpdated = (<Date>last_updated).toISOString();

  const startDateTime = DateTime.fromISO(data_start_date);

  const {
    year: dataStartYear,
    month: dataStartMonth,
    day: dataStartDay,
  } = startDateTime;

  const isStartOfMonth = dataStartDay === 1;

  const isStartOfWeek =
    isStartOfMonth ||
    startDateTime.get("day") === startDateTime.startOf("week").get("day");

  const endDateTime = DateTime.fromISO(data_end_date);

  const isEndOfMonth =
    endDateTime.get("day") === endDateTime.endOf("month").get("day");

  const isEndOfWeek =
    isEndOfMonth ||
    endDateTime.get("day") === endDateTime.endOf("week").get("day");

  const isCompleteMonth = isStartOfMonth && isEndOfMonth;
  const isCompleteWeek = !isCompleteMonth && isStartOfWeek && isEndOfWeek;

  return {
    damaViewId,

    tableSchema,
    tableName,

    lastUpdated,

    state,
    year: dataStartYear,
    month: dataStartMonth,

    isCompleteWeek,
    isCompleteMonth,

    isExpanded: !!is_expanded,

    data_start_date,
    data_end_date,
  };
}

export default async function getEttViewsMetadataSummary(
  dbConn: NodePgDbConnection,
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

  const { rows } = await dbConn.query({
    text: sql,
    values: [damaViewIds],
  });

  const nonEttViews = rows.filter(
    ({ source_name }) => source_name !== NpmrdsTravelTimesImp
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
      const { damaViewId, lastUpdated, data_start_date, data_end_date } = row;

      acc.byViewId[damaViewId] = row;

      if (acc.lastUpdated < lastUpdated) {
        acc.lastUpdated = lastUpdated;
      }

      if (!acc.dataDateRange[0] || acc.dataDateRange[0] > data_start_date) {
        acc.dataDateRange[0] = data_start_date;
      }

      if (!acc.dataDateRange[1] || acc.dataDateRange[1] < data_end_date) {
        acc.dataDateRange[1] = data_end_date;
      }

      return acc;
    },
    {
      byViewId: {},
      dataDateRange: <[string, string]>[
        ettViewsMeta[0].data_start_date,
        ettViewsMeta[0].data_end_date,
      ],
      lastUpdated: ettViewsMeta[0].lastUpdated,
    }
  );

  const sortedByStateThenStartDate = ettViewsMeta
    .sort((rowA, rowB) => {
      const { state: stateA, data_start_date: startDateA } = rowA;
      const { state: stateB, data_start_date: startDateB } = rowB;

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
