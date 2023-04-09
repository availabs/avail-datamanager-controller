import fetch from "node-fetch";
import { Database as SQLiteDB } from "better-sqlite3";
import _ from "lodash";

import dama_events from "data_manager/events";
import logger from "data_manager/logger";

import { isInTaskEtlContext } from "data_manager/contexts";

import {
  getTranscomRequestFormattedTimestamp,
  validateTranscomRequestTimestamp,
  partitionTranscomRequestTimestampsByMonth,
  TranscomApiRequestTimestamp,
} from "../utils/dates";

import TranscomAuthTokenCollector from "../utils/TranscomAuthTokenCollector";

import { RawTranscomEvent } from "../../../domain";

import getEtlContextLocalStateSqliteDb from "../utils/getEtlContextLocalStateSqliteDb";

import { url } from "./data_schema";

const DEFAULT_SLEEP_MS = 10 * 1000; // 10 seconds

export type InitialEvent = {
  type: ":INITIAL";
  payload: {
    start_timestamp: string;
    end_timestamp: string;
  };
};

export type FinalEvent = {
  type: ":FINAL";
  payload: {
    start_timestamp: string;
    end_timestamp: string;
    etl_work_dir: string;
  };
  meta: {
    dama_host_id: string;
  };
};

export async function* makeRawTranscomEventIterator(
  transcom_start_timestamp: string,
  transcom_end_timestamp: string,
  sleepMs: number = DEFAULT_SLEEP_MS
): AsyncGenerator<RawTranscomEvent> {
  validateTranscomRequestTimestamp(transcom_start_timestamp);
  validateTranscomRequestTimestamp(transcom_end_timestamp);

  const partitionedDateTimes = partitionTranscomRequestTimestampsByMonth(
    transcom_start_timestamp,
    transcom_end_timestamp
  );

  const tokenCollector = new TranscomAuthTokenCollector();

  for (const [partitionStartTime, partitionEndTime] of partitionedDateTimes) {
    const reqBody = {
      // See  ../documentation/EventCategoryIds.md
      eventCategoryIds: "1,2,3,4,13",
      eventStatus: "",
      eventType: "",
      state: "",
      county: "",
      city: "",
      reportingOrg: "",
      facility: "",
      primaryLoc: "",
      secondaryLoc: "",
      eventDuration: null,
      startDateTime: partitionStartTime,
      endDateTime: partitionEndTime,
      orgID: "15",
      direction: "",
      iseventbyweekday: 1,
      tripIds: "",
    };

    logger.silly(`reqBody=${JSON.stringify(reqBody, null, 4)}`);

    const authenticationtoken = await tokenCollector.getJWT();

    logger.silly(`authenticationtoken: ${authenticationtoken}`);

    const options = {
      method: "POST",

      headers: {
        "Content-Type": "application/json",
        authenticationtoken: `Bearer ${authenticationtoken}`,
      },

      body: JSON.stringify(reqBody),
    };

    logger.debug("makeRawTranscomEventIterator sending request");

    const response = await fetch(`${url}?userId=78`, options);

    logger.debug("makeRawTranscomEventIterator got response");

    const { data: events } = await response.json();

    logger.debug(`makeRawTranscomEventIterator got ${events?.length} events`);

    if (Array.isArray(events)) {
      for (const event of events) {
        yield event;
      }
    }

    await new Promise((resolve) => setTimeout(resolve, sleepMs));
  }

  await tokenCollector.close();
}

export async function collectTranscomEventsIdsForDateRangeIntoSqliteDb(
  start_ts: TranscomApiRequestTimestamp,
  end_ts: TranscomApiRequestTimestamp,
  sqlite_db: SQLiteDB
) {
  logger.silly(`downloading events from ${start_ts} to ${end_ts}`);

  try {
    sqlite_db.exec("BEGIN ;");

    const insert_stmt = sqlite_db.prepare(`
      INSERT INTO event_ids (id)
        VALUES ( ? )
        ON CONFLICT(id) DO NOTHING
      ;
    `);

    const iter = makeRawTranscomEventIterator(start_ts, end_ts);

    let count = 0;

    for await (const { id } of iter) {
      ++count;

      insert_stmt.run(id);

      if (count % 100 === 0) {
        logger.silly(
          `downloaded events ${
            count - 99
          }-${count} for ${start_ts} to ${end_ts}`
        );
      }
    }

    logger.info(
      `${count} events downloaded for ${start_ts} to ${end_ts} and IDs added to sqlite_db`
    );

    sqlite_db.exec("COMMIT ;");
  } catch (err) {
    sqlite_db.exec("ROLLBACK ;");

    logger.error(`===== ERROR downloading for ${start_ts} to ${end_ts} =====`);

    throw err;
  }
}

export function createEventIdsTable(sqlite_db: SQLiteDB) {
  sqlite_db.exec(`
    CREATE TABLE IF NOT EXISTS event_ids (
      id TEXT PRIMARY KEY
    ) WITHOUT ROWID ;
  `);
}

export function getMonthPartitionsForTimeRange(
  start_timestamp: string | Date,
  end_timestamp: string | Date
) {
  const transcom_start_timestamp: TranscomApiRequestTimestamp =
    getTranscomRequestFormattedTimestamp(start_timestamp);

  const transcom_end_timestamp: TranscomApiRequestTimestamp =
    getTranscomRequestFormattedTimestamp(end_timestamp);

  const month_partitions = partitionTranscomRequestTimestampsByMonth(
    transcom_start_timestamp,
    transcom_end_timestamp
  );

  return month_partitions;
}

export async function collectTranscomEventIdsForTimeRange(
  start_timestamp: string | Date,
  end_timestamp: string | Date
): Promise<FinalEvent> {
  if (!isInTaskEtlContext()) {
    throw new Error("MUST run in a TaskEtlContext");
  }

  try {
    const task_done_type = ":COLLECT_TRANSCOM_EVENT_IDS_DONE";

    const events = await dama_events.getAllEtlContextEvents();

    let task_done_event = events.find((e) => e.type === task_done_type);

    if (task_done_event) {
      logger.warn("Task already DONE");
      return task_done_event;
    }

    const month_partitions = getMonthPartitionsForTimeRange(
      start_timestamp,
      end_timestamp
    );

    logger.info(
      `downloading TRANCOM events for ${start_timestamp} to ${end_timestamp}`
    );

    logger.silly(
      `starting download of month_partitions ${JSON.stringify(
        month_partitions,
        null,
        4
      )}`
    );

    const sqlite_db = getEtlContextLocalStateSqliteDb();

    createEventIdsTable(sqlite_db);

    for (const [start_ts, end_ts] of month_partitions) {
      const download_done_event = {
        type: ":TRANSCOM_EVENTS_DOWNLOADED",
        payload: {
          start_ts,
          end_ts,
        },
      };

      if (events.some((e) => _.isEqual(e, download_done_event))) {
        logger.debug(
          `IDEMPOTENCY: TRANSCOM events already downloaded for ${start_ts} to ${end_ts}`
        );

        continue;
      }

      await collectTranscomEventsIdsForDateRangeIntoSqliteDb(
        start_ts,
        end_ts,
        sqlite_db
      );

      await dama_events.dispatch(download_done_event);
    }

    task_done_event = {
      type: task_done_type,
      payload: {
        start_timestamp,
        end_timestamp,
      },
    };

    await dama_events.dispatch(task_done_event);

    logger.info(
      `${task_done_event} event ${JSON.stringify(task_done_event, null, 4)}`
    );

    sqlite_db.exec("VACUUM ;");

    sqlite_db.close();

    return task_done_event;
  } catch (err) {
    logger.error((<Error>err).message);
    logger.error((<Error>err).stack);

    await dama_events.dispatch({
      type: ":ERROR",
      payload: {
        message: (<Error>err).message,
        stack: (<Error>err).stack,
      },
      error: true,
    });

    throw err;
  }
}
