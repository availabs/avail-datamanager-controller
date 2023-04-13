import { createWriteStream, rmSync } from "fs";
import { createGzip, Gzip } from "zlib";

import fetch from "node-fetch";
import _ from "lodash";

import dama_events from "data_manager/events";
import logger from "data_manager/logger";
import { verifyIsInTaskEtlContext } from "data_manager/contexts";

import { getTimestamp } from "data_utils/time";

import TranscomAuthTokenCollector from "../../utils/TranscomAuthTokenCollector";
import getEtlContextLocalStateSqliteDb from "../../utils/getEtlContextLocalStateSqliteDb";
import { getRawTranscomEventsFilePath } from "../../utils/etlWorkDir";

import { RawTranscomEventExpanded } from "../../domain";

export const url =
  "https://eventsearch.xcmdata.org/HistoricalEventSearch/xcmEvent/getEventById";

export type InitialEvent = {
  type: ":INITIAL";
  payload: {
    etl_work_dir: string;
  };
};

export type FinalEvent = {
  type: ":FINAL";
};

const DEFAULT_BATCH_SIZE = 100;
const DEFAULT_SLEEP_MS = 5 * 1000;

export function createNewEventsWriteStream(etl_work_dir: string) {
  const file_name = `raw-transcom-events-expanded.${getTimestamp()}.ndjson.gz`;

  const file_path = getRawTranscomEventsFilePath(etl_work_dir, file_name);

  const ws = createWriteStream(file_path);
  const gzip = createGzip();

  const is_done = new Promise<void>((resolve, reject) => {
    ws.once("error", reject);
    ws.once("finish", resolve);
  });

  gzip.pipe(ws);

  return { file_name, file_path, write_stream: gzip, is_done };
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
    INSERT INTO downloaded_event ( event_id, file_name )
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

    if (count) {
      // @ts-ignore
      write_stream.end();

      // @ts-ignore
      await is_done;

      sqlite_db.exec("COMMIT ;");
    }

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
      const file_path = getRawTranscomEventsFilePath(etl_work_dir, file_name);

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
