import _ from "lodash";

import {
  PgEnv,
  getConnectedNodePgClient,
} from "data_manager/dama_db/postgres/PostgreSQL";

export type TranscomApiRequestTimestamp = string;

export function decomposeDate(date: Date) {
  const yyyy = date.getFullYear();
  const mm = `0${date.getMonth() + 1}`.slice(-2);
  const dd = `0${date.getDate()}`.slice(-2);
  const HH = `0${date.getHours()}`.slice(-2);
  const MM = `0${date.getMinutes()}`.slice(-2);
  const SS = `0${date.getSeconds()}`.slice(-2);

  return {
    yyyy,
    mm,
    dd,
    HH,
    MM,
    SS,
  };
}

// Date format 'YYYY-MM-DD HH:MI:SS'
export const transcomRequestFormattedTimestamp =
  /^\d{4}-\d{1,2}-\d{1,2} \d{2}:\d{2}:\d{2}$/;

// Required for using timestamp to TRANSCOM API request body.
export function validateTranscomRequestTimestamp(timestamp: string) {
  if (!transcomRequestFormattedTimestamp.test(timestamp)) {
    throw new Error('Timestamps must be in "yyyy-mm-dd HH:MM:SS" format.');
  }
}

export function getTranscomRequestFormattedTimestamp(date: string | Date) {
  date = typeof date === "string" ? new Date(date) : date;

  const { yyyy, mm, dd, HH, MM, SS } = decomposeDate(date);

  return `${yyyy}-${mm}-${dd} ${HH}:${MM}:${SS}`;
}

export function getTimestamp(date: Date) {
  const { yyyy, mm, dd, HH, MM, SS } = decomposeDate(date);

  return `${yyyy}${mm}${dd}T${HH}${MM}${SS}`;
}

export function getDateFromTimestamp(timestamp: string) {
  const yyyy = timestamp.slice(0, 4);
  const mm = timestamp.slice(4, 6);
  const dd = timestamp.slice(6, 8);
  const HH = timestamp.slice(9, 11);
  const MM = timestamp.slice(11, 13);
  const SS = timestamp.slice(13, 15);

  const ts = `${yyyy}-${mm}-${dd}T${HH}:${MM}:${SS}`;

  const date = new Date(ts);

  if (Number.isNaN(date.getTime())) {
    throw new Error(`INVALID TIMESTAMP: ${timestamp}`);
  }

  return date;
}

export function getNowTimestamp(transcomFormat: boolean = false) {
  const now = new Date();

  return transcomFormat
    ? getTranscomRequestFormattedTimestamp(now)
    : getTimestamp(now);
}

export function partitionTranscomRequestTimestampsByMonth(
  startTimestamp: string,
  endTimestamp: string
): [TranscomApiRequestTimestamp, TranscomApiRequestTimestamp][] {
  validateTranscomRequestTimestamp(startTimestamp);
  validateTranscomRequestTimestamp(endTimestamp);

  const [startYearStr] = startTimestamp.split(/-/);
  const [endYearStr] = endTimestamp.split(/-/);

  const startYear = +startYearStr;
  const endYear = +endYearStr;

  const start = new Date(startTimestamp);
  const end = new Date(endTimestamp);

  const startMonth = start.getMonth() + 1;
  const startDate = start.getDate();
  const [, startTime] = startTimestamp.split(" ");

  const endMonth = end.getMonth() + 1;
  const endDate = end.getDate();
  const [, endTime] = endTimestamp.split(" ");

  const partitionedDateTimes = _.range(startYear, endYear + 1).reduce(
    (acc, year) => {
      const isStartYear = year === startYear;
      const isEndYear = year === endYear;

      const s_mm = isStartYear ? startMonth : 1;
      const e_mm = isEndYear ? endMonth : 12;

      for (let mm = s_mm; mm <= e_mm; ++mm) {
        const isStartMonth = isStartYear && mm === startMonth;
        const isEndMonth = isEndYear && mm === endMonth;

        const s_dd = isStartMonth ? startDate : 1;
        const s_time = isStartMonth ? startTime : "00:00:00";

        // FIXME: use luxon
        // NOTE: JavaScript Date months start at zero.
        //       Using date 0 means the last day of the preceeding month.
        //       Therefore, the date below represents the last day of the
        //         mm-th month.
        //       E.G.:
        //              new Date(2021, 1, 0).getDate() === 31
        //
        const e_dd = isEndMonth ? endDate : new Date(year, mm, 0).getDate();

        const e_time = isEndMonth ? endTime : "23:59:59";

        const month = _.padStart(`${mm}`, 2, "0");
        const start_day = _.padStart(`${s_dd}`, 2, "0");
        const end_day = _.padStart(`${e_dd}`, 2, "0");

        acc.push([
          `${year}-${month}-${start_day} ${s_time}`,
          `${year}-${month}-${end_day} ${e_time}`,
        ]);
      }

      return acc;
    },
    <[TranscomApiRequestTimestamp, TranscomApiRequestTimestamp][]>[]
  );

  return partitionedDateTimes;
}

export async function getTranscomEventsMaxCreationTimestamp(
  pgEnv: PgEnv
): Promise<string | null> {
  throw new Error("FIXME: use dama_db");

  const db = await getConnectedNodePgClient(pgEnv);

  try {
    const {
      rows: [start_timestamp = null],
    } = await db.query(`
        SELECT
            to_char(
              MAX(creation),
              'YYYY-MM-DD HH24:MI:SS'
            ) AS latest
          FROM transcom.transcom_historical_events
        ;
      `);

    return start_timestamp;
  } catch (err) {
    console.error(
      "ERROR: Could not connect to retreive the latest event from the database."
    );

    throw err;
  } finally {
    db.end();
  }
}
