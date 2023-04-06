import {
  readdirSync,
  createReadStream,
  createWriteStream,
  mkdirSync,
} from "fs";

import { createGzip, createGunzip } from "zlib";
import { join } from "path";
import { pipeline } from "stream";
import { promisify } from "util";

import fetch from "node-fetch";

import { format as csvFormat, CsvFormatterStream } from "fast-csv";

import _ from "lodash";
import split from "split2";
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

import { getTimestamp } from "data_utils/time";

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

      searchParams: {
        userId: 78,
      },

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
}

export function makeRawTranscomEventIteratorFromApiScrapeFile(
  filePath: string
): AsyncGenerator<RawTranscomEvent> {
  // @ts-ignore
  return pipeline(
    createReadStream(filePath),
    createGunzip(),
    split(JSON.parse),
    (err: any) => {
      if (err) {
        throw err;
      }
    }
  );
}

// NOTE: Assumes file naming pattern /^raw-transcom-events\.*\.ndjson.gz$/
export async function* makeRawTranscomEventIteratorFromApiScrapeDirectory(
  apiScrapeDir: string
) {
  const rawEventFiles = readdirSync(apiScrapeDir)
    .filter((f) => /^raw-transcom-events\..*\.ndjson.gz$/.test(f))
    .sort();

  for (const file of rawEventFiles) {
    console.log(file);
    const rawEventPath = join(apiScrapeDir, file);

    const rawEventIter =
      makeRawTranscomEventIteratorFromApiScrapeFile(rawEventPath);

    for await (const event of rawEventIter) {
      yield event;
    }
  }
}

export async function* makeTranscomEventIdIteratorFromApiScrapeDirectory(
  apiScrapeDir: string
) {
  const rawEventIter =
    makeRawTranscomEventIteratorFromApiScrapeDirectory(apiScrapeDir);

  for await (const { id } of rawEventIter) {
    yield id;
  }
}

export async function loadApiScrapeDirectoryIntoDatabase(
  apiScrapeDir: string,
  schemaName: string,
  tableName: string
) {
  const rawEventIter =
    makeRawTranscomEventIteratorFromApiScrapeDirectory(apiScrapeDir);

  const protoTranscomEventIter =
    transformRawTranscomEventIteratorToProtoTranscomEventIterator(rawEventIter);

  await loadProtoTranscomEventsIntoDatabase(
    protoTranscomEventIter,
    schemaName,
    tableName
  );
}

export async function downloadTranscomEventsForDateRange(
  start_ts: TranscomApiRequestTimestamp,
  end_ts: TranscomApiRequestTimestamp,
  etl_work_dir: string
) {
  const start = start_ts.replace(/-|:/g, "").replace(/ /, "T");
  const end = end_ts.replace(/-|:/g, "").replace(/ /, "T");

  const fname = `raw-transcom-events.${start}-${end}.${getTimestamp()}.ndjson.gz`;
  const fpath = join(etl_work_dir, fname);

  logger.silly(`downloading events ${start_ts} to ${end_ts} into ${fpath}`);

  try {
    const ws = createWriteStream(fpath);
    const gzip = createGzip();

    const done = new Promise((resolve, reject) => {
      ws.once("error", reject);
      ws.once("finish", resolve);
    });

    gzip.pipe(ws);

    const iter = makeRawTranscomEventIterator(start_ts, end_ts);

    let count = 0;

    for await (const event of iter) {
      ++count;

      const ready = gzip.write(`${JSON.stringify(event)}\n`);

      if (!ready) {
        await new Promise((resolve) => gzip.once("drain", resolve));
      }

      if (count % 100 === 0) {
        logger.silly(
          `downloaded events ${
            count - 99
          }-${count} for ${start_ts} to ${end_ts}`
        );
      }
    }

    gzip.end();

    await done;

    logger.info(
      `${count} events downloaded for ${start_ts} to ${end_ts} into ${fname}`
    );

    return fpath;
  } catch (err) {
    logger.error(`===== ERROR downloading ${fname} =====`);
    throw err;
  }
}

export default async function main(
  initial_event: InitialEvent
): Promise<FinalEvent> {
  if (!isInTaskEtlContext) {
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

      await downloadTranscomEventsForDateRange(start_ts, end_ts, etl_work_dir);

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
