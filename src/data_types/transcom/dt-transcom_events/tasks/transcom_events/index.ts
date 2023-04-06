import { mkdirSync } from "fs";

import { join } from "path";
import { pipeline } from "stream";
import { promisify } from "util";

import fetch from "node-fetch";
import BetterSQLite, { Database as SQLiteDB } from "better-sqlite3";

import { format as csvFormat, CsvFormatterStream } from "fast-csv";

import _ from "lodash";
import pgFormat from "pg-format";
import { from as copyFrom } from "pg-copy-streams";

import dama_host_id from "constants/damaHostId";
import etl_dir from "constants/etlDir";

import dama_db from "data_manager/dama_db";
import dama_events from "data_manager/events";
import logger from "data_manager/logger";

import {
  getPgEnv,
  getEtlContextId,
  isInTaskEtlContext,
} from "data_manager/contexts";

import {
  getTranscomRequestFormattedTimestamp,
  validateTranscomRequestTimestamp,
  partitionTranscomRequestTimestampsByMonth,
  TranscomApiRequestTimestamp,
} from "../utils/dates";

import TranscomAuthTokenCollector from "../utils/TranscomAuthTokenCollector";

import { RawTranscomEvent, ProtoTranscomEvent } from "../../../domain";

import { url, apiResponsePropsToDbCols, dbCols } from "./data_schema";

const pipelineAsync = promisify(pipeline);

const DEFAULT_SLEEP_MS = 10 * 1000; // 10 seconds

const sqlite_db_fname = "etl_context_local_state.sqlite3";

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

export function transformRawTranscomEventToProtoTranscomEvent(
  rawTranscomEvent: RawTranscomEvent
): ProtoTranscomEvent {
  const d = _(rawTranscomEvent)
    .mapKeys((_v, k) => apiResponsePropsToDbCols[k])
    .mapValues((v) => (v === "" ? null : v))
    .value();

  d.event_type =
    typeof d.event_type === "string"
      ? d.event_type?.toLowerCase()
      : d.event_type;

  d.direction =
    typeof d.direction === "string" ? d.direction?.toLowerCase() : d.direction;

  d.event_status =
    typeof d.event_status === "string"
      ? d.event_status?.toLowerCase()
      : d.event_status;

  return <ProtoTranscomEvent>d;
}

export async function* transformRawTranscomEventIteratorToProtoTranscomEventIterator(
  rawTranscomEventIter: AsyncGenerator<RawTranscomEvent>
): AsyncGenerator<ProtoTranscomEvent> {
  for await (const data of rawTranscomEventIter) {
    yield transformRawTranscomEventToProtoTranscomEvent(data);
  }
}

export function protoTranscomEventIteratorToCsvStream(
  protoTranscomEventIter: AsyncGenerator<ProtoTranscomEvent>
): CsvFormatterStream<ProtoTranscomEvent, any> {
  const csvStream = csvFormat({
    headers: dbCols,
    quoteHeaders: false,
    quote: '"',
  });

  process.nextTick(async () => {
    for await (const event of protoTranscomEventIter) {
      const ready = csvStream.write(event);

      if (!ready) {
        await new Promise((resolve) => csvStream.once("drain", resolve));
        console.error("drain event");
      }
    }

    csvStream.end();
  });

  return csvStream;
}

export async function loadProtoTranscomEventsIntoDatabase(
  protoTranscomEventIter: AsyncGenerator<ProtoTranscomEvent>,
  schemaName: string,
  tableName: string
) {
  // NOTE: using the transcomEventsDatabaseTableColumns array
  //       keeps column order consistent with transcomEventsCsvStream
  const colIdentifiers = dbCols.slice().fill("%I").join();

  const sql = pgFormat(
    `COPY %I.%I (${colIdentifiers}) FROM STDIN WITH CSV HEADER ;`,
    schemaName,
    tableName,
    ...dbCols
  );

  const db = await dama_db.getDbConnection();

  const pgCopyStream = db.query(copyFrom(sql));

  const csvStream = protoTranscomEventIteratorToCsvStream(
    protoTranscomEventIter
  );

  await pipelineAsync(csvStream, pgCopyStream);

  db.release();
}

export async function insertTranscomEventsIdsForDateRangeIntoSqliteDb(
  start_ts: TranscomApiRequestTimestamp,
  end_ts: TranscomApiRequestTimestamp,
  sqlite_db: SQLiteDB
) {
  logger.silly(`downloading events from ${start_ts} to ${end_ts}`);

  try {
    sqlite_db.exec("BEGIN ;");

    sqlite_db.exec(`
      CREATE TABLE IF NOT EXISTS event_ids (
        id TEXT PRIMARY KEY
      ) WITHOUT ROWID ;
    `);

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
      `${count} events downloaded for ${start_ts} to ${end_ts} and IDs added to ${sqlite_db_fname}`
    );

    sqlite_db.exec("COMMIT ;");
  } catch (err) {
    logger.error(`===== ERROR downloading for ${start_ts} to ${end_ts} =====`);
    logger.error((<Error>err).message);
    logger.error((<Error>err).stack);

    sqlite_db.exec("ROLLBACK ;");
    throw err;
  }
}

export async function* makeTranscomEventIdIterator() {
  if (!isInTaskEtlContext()) {
    throw new Error("MUST run in a TaskEtlContext");
  }

  const etl_work_dir = join(
    etl_dir,
    `transcom.download_transcom_events.pg_env_${getPgEnv()}.etl_context_${getEtlContextId()}`
  );

  const sqlite_db = new BetterSQLite(join(etl_work_dir, sqlite_db_fname));

  const stmt = sqlite_db.prepare(`
    SELECT
        id
      FROM event_ids
      ORDER BY id
  `);

  for await (const { id } of stmt.iterate()) {
    yield id;
    await new Promise((resolve) => process.nextTick(resolve));
  }
}

export default async function main(
  initial_event: InitialEvent
): Promise<FinalEvent> {
  if (!isInTaskEtlContext()) {
    throw new Error("MUST run in a TaskEtlContext");
  }

  try {
    const {
      payload: { start_timestamp, end_timestamp },
    }: InitialEvent = initial_event;

    const events = await dama_events.getAllEtlContextEvents();

    let final_event = events.find((e) => /:FINAL$/.test(e.type));

    if (final_event) {
      logger.warn("Task already DONE");
      return final_event;
    }

    logger.info(`:INITIAL event ${JSON.stringify(initial_event, null, 4)}`);

    const etl_work_dir = join(
      etl_dir,
      `transcom.download_transcom_events.pg_env_${getPgEnv()}.etl_context_${getEtlContextId()}`
    );

    mkdirSync(etl_work_dir, { recursive: true });

    const transcom_start_timestamp: TranscomApiRequestTimestamp =
      getTranscomRequestFormattedTimestamp(start_timestamp);

    const transcom_end_timestamp: TranscomApiRequestTimestamp =
      getTranscomRequestFormattedTimestamp(end_timestamp);

    const month_partitions = partitionTranscomRequestTimestampsByMonth(
      transcom_start_timestamp,
      transcom_end_timestamp
    );

    logger.info(
      `downloading TRANCOM events for ${transcom_start_timestamp} to ${transcom_end_timestamp}`
    );

    logger.silly(
      `starting download of month_partitions ${JSON.stringify(
        month_partitions,
        null,
        4
      )}`
    );

    const sqlite_db = new BetterSQLite(join(etl_work_dir, sqlite_db_fname));

    for (const [start_ts, end_ts] of month_partitions) {
      const download_done_event = {
        type: ":TRANSCOM_EVENTS_DOWNLOADED",
        // @ts-ignore
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

      await insertTranscomEventsIdsForDateRangeIntoSqliteDb(
        start_ts,
        end_ts,
        sqlite_db
      );

      // @ts-ignore
      await dama_events.dispatch(download_done_event);
    }

    final_event = {
      type: ":FINAL",
      payload: {
        start_timestamp,
        end_timestamp,
        etl_work_dir,
      },
      meta: {
        dama_host_id,
      },
    };

    // @ts-ignore
    await dama_events.dispatch(final_event);

    sqlite_db.exec("VACUUM ;");

    sqlite_db.close();

    return final_event;
  } catch (err) {
    logger.error((<Error>err).message);
    logger.error((<Error>err).stack);

    await dama_events.dispatch({
      type: ":ERROR",
      // @ts-ignore
      payload: {
        message: (<Error>err).message,
        stack: (<Error>err).stack,
      },
      error: true,
    });

    throw err;
  }
}
