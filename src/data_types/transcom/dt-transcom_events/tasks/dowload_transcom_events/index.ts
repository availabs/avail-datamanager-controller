import { createReadStream, mkdirSync, createWriteStream, rmSync } from "fs";
import { createGzip, createGunzip, Gzip } from "zlib";
import { join } from "path";
import { pipeline } from "stream";
import { promisify } from "util";

import fetch from "node-fetch";
import { format as csvFormat, CsvFormatterStream } from "fast-csv";
import _ from "lodash";
import split from "split2";
import pgFormat from "pg-format";
import { from as copyFrom } from "pg-copy-streams";

import dama_db from "data_manager/dama_db";
import dama_events from "data_manager/events";
import logger from "data_manager/logger";
import { verifyIsInTaskEtlContext } from "data_manager/contexts";

import TranscomAuthTokenCollector from "../utils/TranscomAuthTokenCollector";
import getEtlContextLocalStateSqliteDb from "../utils/getEtlContextLocalStateSqliteDb";

// NOTE: dbCols array ensures same order of columns in CSV and COPY FROM CSV statement.
import { url, apiResponsePropsToDbCols, dbCols } from "./data_schema";

import {
  RawTranscomEventExpanded,
  ProtoTranscomEventExpanded,
} from "../../domain";

import { getTimestamp } from "data_utils/time";

export type InitialEvent = {
  type: ":INITIAL";
  payload: {
    etl_work_dir: string;
  };
};

export type FinalEvent = {
  type: ":FINAL";
};

const pipelineAsync = promisify(pipeline);

const DEFAULT_BATCH_SIZE = 100;
const DEFAULT_SLEEP_MS = 5 * 1000;

export function getRawTranscomEventsExpandedDownloadDir(etl_work_dir: string) {
  const events_dir = join(etl_work_dir, "raw-transcom-events-expanded");
  mkdirSync(events_dir, { recursive: true });

  return events_dir;
}

export function getRawTranscomEventsExpandedFilePath(
  etl_work_dir: string,
  file_name: string
) {
  const events_dir = getRawTranscomEventsExpandedDownloadDir(etl_work_dir);

  return join(events_dir, file_name);
}

export async function downloadRawTranscomEventsExpanded(
  transcom_event_ids: string[],
  jwt: string
): Promise<RawTranscomEventExpanded[]> {
  if (transcom_event_ids.length === 0) {
    return [];
  }

  const options = {
    headers: {
      "Content-Type": "application/json",
      authenticationtoken: `Bearer ${jwt}`,
    },
  };

  const reqUrl = `${url}?id=${transcom_event_ids.join("&id=")}`;

  const response = await fetch(reqUrl, options);

  // @ts-ignore
  const { data } = await response.json();

  logger.silly(JSON.stringify({ data }, null, 4));

  return data;
}

// TODO: validator function that wraps the Iterator
export async function* makeRawTranscomEventsExpandedIteratorFromTranscomAPI(
  etl_work_dir: string,
  batch_size: number = DEFAULT_BATCH_SIZE,
  sleep_ms: number = DEFAULT_SLEEP_MS
): AsyncGenerator<RawTranscomEventExpanded> {
  const sqlite_db = getEtlContextLocalStateSqliteDb(etl_work_dir);

  const select_batch_ids_stmt = sqlite_db.prepare(`
      SELECT
          event_id
        FROM seen_event
      EXCEPT
      SELECT
          event_id
        FROM downloaded_event
      ORDER BY event_id
      LIMIT ${batch_size}
    ;
  `);

  const tokenCollector = new TranscomAuthTokenCollector();

  let mustSleep = false;

  while (true) {
    const batch = select_batch_ids_stmt.pluck().all();

    logger.silly(JSON.stringify({ batch }, null, 4));

    if (batch.length === 0) {
      break;
    }

    if (mustSleep) {
      await new Promise((resolve) => setTimeout(resolve, sleep_ms));
      mustSleep = false;
    }

    const jwt = await tokenCollector.getJWT();

    const eventsExpandedData = await downloadRawTranscomEventsExpanded(
      batch,
      jwt
    );

    mustSleep = true;

    for (const data of eventsExpandedData) {
      yield data;
    }
  }

  await tokenCollector.close();
}

export function transformRawTranscomEventExpandedToProtoTranscomEventExpanded(
  e: RawTranscomEventExpanded
): ProtoTranscomEventExpanded {
  // TODO: TEST
  return <ProtoTranscomEventExpanded>(
    _.mapKeys(e, (_v, k) => apiResponsePropsToDbCols[k])
  );
}

export async function* transformRawTranscomEventExpandedIteratorToProtoTranscomEventExpandedIterator(
  rawTranscomEventExpandedIter:
    | Iterable<RawTranscomEventExpanded>
    | AsyncIterable<RawTranscomEventExpanded>
): AsyncGenerator<ProtoTranscomEventExpanded> {
  for await (const event of rawTranscomEventExpandedIter) {
    yield transformRawTranscomEventExpandedToProtoTranscomEventExpanded(event);
  }
}

export function protoTranscomEventExpandedIteratorToCsvStream(
  protoTranscomEventExpandedIter: AsyncGenerator<ProtoTranscomEventExpanded>
): CsvFormatterStream<ProtoTranscomEventExpanded, any> {
  const csv_stream = csvFormat({
    headers: dbCols,
    quoteHeaders: false,
    quote: '"',
  });

  process.nextTick(async () => {
    for await (const event of protoTranscomEventExpandedIter) {
      const ready = csv_stream.write(event);

      if (!ready) {
        await new Promise((resolve) => csv_stream.once("drain", resolve));
        console.error("drain event");
      }
    }

    csv_stream.end();
  });

  return csv_stream;
}

export async function loadProtoTranscomEventsExpandedIntoDatabase(
  protoTranscomEventIter: AsyncGenerator<ProtoTranscomEventExpanded>,
  table_schema: string,
  table_name: string
) {
  // NOTE: using the transcomEventsDatabaseTableColumns array
  //       keeps column order consistent with transcomEventsCsvStream
  const column_identifiers = dbCols.slice().fill("%I").join();

  const sql = pgFormat(
    `COPY %I.%I (${column_identifiers}) FROM STDIN WITH CSV HEADER ;`,
    table_schema,
    table_name,
    ...dbCols
  );

  const db = await dama_db.getDbConnection();

  try {
    const pg_copy_stream = db.query(copyFrom(sql));

    const csv_stream = protoTranscomEventExpandedIteratorToCsvStream(
      protoTranscomEventIter
    );

    await pipelineAsync(csv_stream, pg_copy_stream);
  } catch (err) {
    logger.error((<Error>err).message);
    logger.error((<Error>err).stack);
    throw err;
  } finally {
    db.release();
  }
}

export function makeRawTranscomEventExpandedIteratorFromApiScrapeFile(
  filePath: string
): AsyncGenerator<RawTranscomEventExpanded> {
  // @ts-ignore
  return pipeline(
    createReadStream(filePath),
    createGunzip(),
    split(JSON.parse),
    (err) => {
      if (err) {
        logger.error((<Error>err).message);
        logger.error((<Error>err).stack);
        throw err;
      }
    }
  );
}

// NOTE: Assumes file naming pattern /^raw-transcom-events\.*\.ndjson.gz$/
export async function* makeRawTranscomEventsExpandedIteratorFromApiScrapeDirectory(
  etl_work_dir: string
) {
  const sqlite_db = getEtlContextLocalStateSqliteDb(etl_work_dir);

  const file_paths_iter = sqlite_db
    .prepare(
      `
        SELECT DISTINCT
           file_name
          FROM downloaded_event
          ORDER BY 1
      `
    )
    .pluck()
    .iterate();

  for (const file_name of file_paths_iter) {
    const file_path = getRawTranscomEventsExpandedFilePath(
      etl_work_dir,
      file_name
    );

    const rawEventsIter =
      makeRawTranscomEventExpandedIteratorFromApiScrapeFile(file_path);

    for await (const event of rawEventsIter) {
      yield event;
    }
  }
}

export async function loadApiScrapeDirectoryIntoDatabase(
  etl_work_dir: string,
  table_schema: string,
  table_name: string
) {
  const rawEventsIter =
    makeRawTranscomEventsExpandedIteratorFromApiScrapeDirectory(etl_work_dir);

  const protoTranscomEventIter =
    transformRawTranscomEventExpandedIteratorToProtoTranscomEventExpandedIterator(
      rawEventsIter
    );

  await loadProtoTranscomEventsExpandedIntoDatabase(
    protoTranscomEventIter,
    table_schema,
    table_name
  );
}

export function createNewEventsWriteStream(etl_work_dir: string) {
  const file_name = `raw-transcom-events-expanded.${getTimestamp()}.ndjson.gz`;

  const file_path = getRawTranscomEventsExpandedFilePath(
    etl_work_dir,
    file_name
  );

  const ws = createWriteStream(file_path);
  const gzip = createGzip();

  const is_done = new Promise<void>((resolve, reject) => {
    ws.once("error", reject);
    ws.once("finish", resolve);
  });

  gzip.pipe(ws);

  return { file_name, file_path, write_stream: gzip, is_done };
}

export default async function downloadTranscomEventsExpanded(
  etl_work_dir: string,
  batch_size: number = DEFAULT_BATCH_SIZE,
  sleep_ms: number = DEFAULT_SLEEP_MS
) {
  verifyIsInTaskEtlContext();

  const events = await dama_events.getAllEtlContextEvents();

  let final_event = events.find((e) => e.type === ":FINAL");

  if (final_event) {
    logger.info("Task already DONE");
    return final_event;
  }

  const sqlite_db = getEtlContextLocalStateSqliteDb(etl_work_dir);

  const insert_downloaded_id_stmt = sqlite_db.prepare(`
    INSERT INTO downloaded_event ( event_id, file_path )
      VALUES ( ?, ? )
      ON CONFLICT( event_id ) DO NOTHING
    ;
  `);

  const iter = makeRawTranscomEventsExpandedIteratorFromTranscomAPI(
    etl_work_dir,
    batch_size,
    sleep_ms
  );

  let file_name: string;
  let write_stream: Gzip;
  let is_done: Promise<void>;

  let count = 0;
  try {
    for await (const event of iter) {
      if (count++ % batch_size === 0) {
        // @ts-ignore
        if (write_stream) {
          write_stream.end();
          // @ts-ignore
          await is_done;

          sqlite_db.exec("COMMIT ;");
        }

        ({ file_name, write_stream, is_done } =
          createNewEventsWriteStream(etl_work_dir));

        sqlite_db.exec("BEGIN ;");
      }

      // @ts-ignore
      const ready = write_stream.write(`${JSON.stringify(event)}\n`);

      if (!ready) {
        await new Promise((resolve) => write_stream.once("drain", resolve));
      }

      const { ID } = event;

      // @ts-ignore
      insert_downloaded_id_stmt.run(ID, file_name);
    }

    // @ts-ignore
    write_stream.end();

    // @ts-ignore
    await is_done;

    sqlite_db.exec("COMMIT ;");

    logger.info(`Downloaded ${count} events."`);

    final_event = {
      type: ":FINAL",
    };

    await dama_events.dispatch(final_event);

    return final_event;
  } catch (err) {
    console.error(err);
    // sqlite_db.exec("ROLLBACK; ");

    // @ts-ignore
    if (file_name) {
      const file_path = getRawTranscomEventsExpandedFilePath(
        etl_work_dir,
        file_name
      );

      rmSync(file_path);
    }

    const err_event = {
      type: ":ERROR",
      payload: {
        message: (<Error>err).message,
        stack: (<Error>err).stack,
      },
    };

    await dama_events.dispatch(err_event);

    throw err;
  }
}
