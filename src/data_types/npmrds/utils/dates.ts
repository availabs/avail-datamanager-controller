import _ from "lodash";
import { DateTime } from "luxon";

import { DataDate, DataDateRange } from "../domain";

export const iso_date_re = /^\d{4}-\d{2}-\d{2}$/;

export function createDataDate(
  date: string,
  time_of_day: "start" | "end"
): DataDate {
  const date_time = DateTime.fromISO(date)[`${time_of_day}Of`]("day");

  if (!date_time.isValid) {
    throw new Error(`Invalid date: ${date}`);
  }

  return {
    year: date_time.year,
    month: date_time.month,
    day: date_time.day,

    iso: date_time.toISO(),
    iso_date: date_time.toISODate(),
  };
}

export function createDataDateRange(
  start_date: string,
  end_date: string,
  enforce_weekly_or_monthly = false
): DataDateRange {
  const start_data_date = createDataDate(start_date, "start");
  const end_data_date = createDataDate(end_date, "end");

  const start_date_time = DateTime.fromISO(start_data_date.iso);
  const end_date_time = DateTime.fromISO(end_data_date.iso);

  const start_is_start_of_month = start_data_date.day === 1;
  const start_is_start_of_week =
    start_data_date.day === start_date_time.startOf("week").day;

  const end_is_end_of_month =
    end_data_date.day === end_date_time.endOf("month").day;
  const end_is_end_of_week =
    end_data_date.day === end_date_time.endOf("week").day;

  const date_range = {
    start: {
      ...start_data_date,

      is_start_of_month: start_is_start_of_month,
      is_start_of_week: start_is_start_of_week,
    },

    end: {
      ...end_data_date,

      is_end_of_month: end_is_end_of_month,
      is_end_of_week: end_is_end_of_week,
    },
  };

  validateDateRange(date_range, enforce_weekly_or_monthly);

  return date_range;
}

export function validateDateRange(
  date_range: DataDateRange,
  enforce_weekly_or_monthly = false
) {
  const { start, end } = date_range;

  const start_date_time = DateTime.fromISO(start.iso);
  const end_date_time = DateTime.fromISO(end.iso);

  const error_messages: string[] = [];

  if (!start_date_time.isValid) {
    error_messages.push("Invalid start date");
  }

  if (!end_date_time.isValid) {
    error_messages.push("Invalid end date");
  }

  // If the dates are invalid, can't validate further.
  if (error_messages.length) {
    throw new Error(`Invalid date range: ${error_messages.join("\n\t* ")}`);
  }

  if (start_date_time > end_date_time) {
    throw new Error(`Start date follows end date: ${start.iso} to ${end.iso}`);
  }

  if (enforce_weekly_or_monthly) {
    if (!(start.is_start_of_month || start.is_start_of_week)) {
      error_messages.push(
        "Start dates MUST be the beginning of a week or month (Monday)."
      );
    }

    if (!(end.is_end_of_month || end.is_end_of_week)) {
      error_messages.push(
        "End dates MUST be the end of a week or month (Sunday)."
      );
    }
  }

  if (error_messages.length) {
    throw new Error(
      `Invalid date range: ${start.iso} to ${end.iso}\n${error_messages.join(
        "\n\t* "
      )}`
    );
  }
}

export function partitionDateRange(
  start_date: string,
  end_date: string
): { start_date: string; end_date: string }[] {
  const { start, end } = createDataDateRange(start_date, end_date, true);

  const partitions: DataDateRange[] = [];

  const end_date_time = DateTime.fromISO(end.iso);

  let cur_start_date_time = DateTime.fromISO(start.iso);

  // NOTE: start_date_time is always midnight. end_date_time is always 23:59:59
  while (cur_start_date_time < end_date_time) {
    const cur_start_date = cur_start_date_time.toISODate();

    const cur_start_is_start_of_month = cur_start_date_time.day === 1;
    const cur_start_is_start_of_week =
      cur_start_date_time === cur_start_date_time.startOf("week");

    const cur_end_of_week = cur_start_date_time.endOf("week");
    const cur_end_of_month = cur_start_date_time.endOf("month");

    let cur_end_date_time: DateTime;

    if (cur_start_is_start_of_month && cur_end_of_month <= end_date_time) {
      // We can request the entire month
      cur_end_date_time = cur_end_of_month;
    } else {
      // We must request a week of data, but stay within the calendar month
      cur_end_date_time =
        cur_end_of_month < cur_end_of_week ? cur_end_of_month : cur_end_of_week;
    }

    if (cur_end_date_time > end_date_time) {
      throw new Error(
        "INVARIANT BROKEN: end_date must be an end of month or week"
      );
    }

    const cur_end_date = cur_end_date_time.toISODate();
    const cur_end_is_end_of_month = true;
    const cur_end_is_end_of_week =
      cur_end_date_time === cur_end_date_time.endOf("week");

    const cur_date_range = {
      start: {
        date: cur_start_date,
        date_time: cur_start_date_time,

        year: cur_start_date_time.year,
        month: cur_start_date_time.month,
        day: cur_start_date_time.day,

        iso: cur_start_date_time.toISO(),
        iso_date: cur_start_date_time.toISODate(),

        is_start_of_month: cur_start_is_start_of_month,
        is_start_of_week: cur_start_is_start_of_week,
      },

      end: {
        date: cur_end_date,
        date_time: cur_end_date_time,

        year: cur_end_date_time.year,
        month: cur_end_date_time.month,
        day: cur_end_date_time.day,

        iso: cur_end_date_time.toISO(),
        iso_date: cur_end_date_time.toISODate(),

        is_end_of_month: cur_end_is_end_of_month,
        is_end_of_week: cur_end_is_end_of_week,
      },
    };

    partitions.push(cur_date_range);

    cur_start_date_time = cur_end_date_time.startOf("day").plus({ day: 1 });
  }

  return partitions.map(
    ({
      start: { iso_date: start_iso_date },
      end: { iso_date: end_iso_date },
    }) => ({
      start_date: start_iso_date,
      end_date: end_iso_date,
    })
  );
}
