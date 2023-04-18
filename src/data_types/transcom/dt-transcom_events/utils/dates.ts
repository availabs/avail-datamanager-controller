import _ from "lodash";
import { DateTime } from "luxon";

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

// The TRANSCOM API requires the following timestamp format: 'YYYY-MM-DD HH:MI:SS'
export const transcomRequestFormattedTimestamp =
  /^\d{4}-\d{1,2}-\d{1,2} \d{2}:\d{2}:\d{2}$/;

/**
 * Checks that a timestamp is in the required TRANSCOM 'YYYY-MM-DD HH:MI:SS' format.
 *
 * @param timestamp - timestamp whose format to verify
 *
 * @throws Throw an error if the timestamp is not in the TRANSCOM required 'YYYY-MM-DD HH:MI:SS' format.
 */
export function validateTranscomRequestTimestamp(timestamp: string) {
  if (!transcomRequestFormattedTimestamp.test(timestamp)) {
    throw new Error('Timestamps must be in "yyyy-mm-dd HH:MM:SS" format.');
  }
}

/**
 * Get the TRANSCOM 'YYYY-MM-DD HH:MI:SS' format timestamp for the date.
 *
 * @param time - Date or timestamp to convert to TRANSCOM timestamp format.
 *
 * @returns The time formatted in TRANSCOM's required format.
 */
export function getTranscomRequestFormattedTimestamp(time: string | Date) {
  const dt =
    typeof time === "string"
      ? DateTime.fromISO(time)
      : DateTime.fromJSDate(time);

  return dt.toFormat("yyyy-MM-dd HH:mm:ss");
}

export function getTimestamp(date: Date) {
  const { yyyy, mm, dd, HH, MM, SS } = decomposeDate(date);

  return `${yyyy}${mm}${dd}T${HH}${MM}${SS}`;
}

/**
 *  This function partitions a date range into months.
 *
 *  @remarks
 *    We politely request the non-expanded TRANSCOM events one month at a time.
 *
 * @param start_timestamp
 * @param end_timestamp
 *
 * @returns An Array of [start_timestamp, end_timestamp] pairs. No pair extends beyond a calendar month.
 *
 * @throws If either start_timestamp or end_timestamp are not in the TRANSCOM 'YYYY-MM-DD HH:MI:SS' format.
 */
export function partitionTranscomRequestTimestampsByMonth(
  start_timestamp: string,
  end_timestamp: string
): [TranscomApiRequestTimestamp, TranscomApiRequestTimestamp][] {
  validateTranscomRequestTimestamp(start_timestamp);
  validateTranscomRequestTimestamp(end_timestamp);

  const [startYearStr] = start_timestamp.split(/-/);
  const [endYearStr] = end_timestamp.split(/-/);

  const startYear = +startYearStr;
  const endYear = +endYearStr;

  const start = new Date(start_timestamp);
  const end = new Date(end_timestamp);

  const startMonth = start.getMonth() + 1;
  const startDate = start.getDate();
  const [, startTime] = start_timestamp.split(" ");

  const endMonth = end.getMonth() + 1;
  const endDate = end.getDate();
  const [, endTime] = end_timestamp.split(" ");

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
