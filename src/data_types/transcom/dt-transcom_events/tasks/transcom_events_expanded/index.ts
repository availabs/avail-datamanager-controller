import {
  readdirSync,
  createReadStream,
  mkdirSync,
  createWriteStream,
  readFileSync,
} from "fs";
import { createGzip, createGunzip } from "zlib";
import { join } from "path";
import { pipeline } from "stream";
import { promisify } from "util";

import got from "got";
import { format as csvFormat, CsvFormatterStream } from "fast-csv";

import _ from "lodash";
import split from "split2";
import pgFormat from "pg-format";
import { from as copyFrom } from "pg-copy-streams";

import { Client } from "pg";

import { url, apiResponsePropsToDbCols, dbCols } from "./data_schema";

import { getNowTimestamp } from "../utils/dates";

import {
  TranscomEventID,
  RawTranscomEventExpanded,
  ProtoTranscomEventExpanded,
} from "./index.d";

const pipelineAsync = promisify(pipeline);

const DEFAULT_BATCH_SIZE = 50;
const DEFAULT_SLEEP_MS = 10 * 1000; // 10 seconds

function getRawTranscomEventsExpandedFileName() {
  return `raw-transcom-events-expanded.${getNowTimestamp()}.ndjson.gz`;
}

function getAuthenticationToken() {
  const p = join(__dirname, "../transcom_events/config/authenticationtoken");
  const t = readFileSync(p, { encoding: "utf8" }).trim();

  return t;
}

export async function downloadRawTranscomEventExpanded(
  transcomEventIds: string[]
): Promise<RawTranscomEventExpanded[]> {
  if (transcomEventIds.length === 0) {
    return [];
  }

  const authenticationtoken = getAuthenticationToken();

  console.log("authenticationtoken:", authenticationtoken);

  const options = {
    headers: {
      authenticationtoken,
    },
  };

  const reqUrl = `${url}?id=${transcomEventIds.join("&id=")}`;

  const response = await got.get(reqUrl, options).json();

  // @ts-ignore
  const { data } = response;

  return data;
}

// TODO: validator function that wraps the Iterator
export async function* makeRawTranscomEventsExpandedIteratorFromTranscomAPI(
  transcomEventIdsIter:
    | Iterable<TranscomEventID>
    | AsyncIterable<TranscomEventID>,
  batchSize: number = DEFAULT_BATCH_SIZE,
  sleepMs: number = DEFAULT_SLEEP_MS
): AsyncGenerator<RawTranscomEventExpanded> {
  const batch: TranscomEventID[] = [];

  let mustSleep = false;
  for await (const id of transcomEventIdsIter) {
    batch.push(id);

    if (batch.length === batchSize) {
      if (mustSleep) {
        await new Promise((resolve) => setTimeout(resolve, sleepMs));
        mustSleep = false;
      }

      const eventsExpandedData = await downloadRawTranscomEventExpanded(batch);
      mustSleep = true;

      batch.length = 0;

      for (const data of eventsExpandedData) {
        yield data;
      }
    }
  }

  if (batch.length) {
    if (mustSleep) {
      await new Promise((resolve) => setTimeout(resolve, sleepMs));
      mustSleep = false;
    }

    const eventsExpandedData = await downloadRawTranscomEventExpanded(batch);
    mustSleep = true;

    for (const data of eventsExpandedData) {
      yield data;
    }
  }
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
  const csvStream = csvFormat({
    headers: dbCols,
    quoteHeaders: false,
    quote: '"',
  });

  process.nextTick(async () => {
    for await (const event of protoTranscomEventExpandedIter) {
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

export async function loadProtoTranscomEventsExpandedIntoDatabase(
  protoTranscomEventIter: AsyncGenerator<ProtoTranscomEventExpanded>,
  schemaName: string,
  tableName: string,
  db: Client
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

  const pgCopyStream = db.query(copyFrom(sql));

  const csvStream = protoTranscomEventExpandedIteratorToCsvStream(
    protoTranscomEventIter
  );

  await pipelineAsync(csvStream, pgCopyStream);
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
        throw err;
      }
    }
  );
}

// NOTE: Assumes file naming pattern /^raw-transcom-events\.*\.ndjson.gz$/
export async function* makeRawTranscomEventsExpandedIteratorFromApiScrapeDirectory(
  apiScrapeDir: string
) {
  const rawEventFiles = readdirSync(apiScrapeDir)
    .filter((f) => /^raw-transcom-events-expanded\..*\.ndjson.gz$/.test(f))
    .sort();

  for (const file of rawEventFiles) {
    console.log(file);
    const rawEventPath = join(apiScrapeDir, file);

    const rawEventsIter =
      makeRawTranscomEventExpandedIteratorFromApiScrapeFile(rawEventPath);

    for await (const event of rawEventsIter) {
      yield event;
    }
  }
}

export async function downloadTranscomEventsExpanded(
  transcomEventIdsIter:
    | Iterable<TranscomEventID>
    | AsyncIterable<TranscomEventID>,
  outputDir: string,
  batchSize: number = DEFAULT_BATCH_SIZE,
  sleepMs: number = DEFAULT_SLEEP_MS
) {
  mkdirSync(outputDir, { recursive: true });

  const filename = getRawTranscomEventsExpandedFileName();
  const filepath = join(outputDir, filename);

  const ws = createWriteStream(filepath);
  const gzip = createGzip();

  const done = new Promise((resolve, reject) => {
    ws.once("error", reject);
    ws.once("finish", resolve);
  });

  gzip.pipe(ws);

  const iter = makeRawTranscomEventsExpandedIteratorFromTranscomAPI(
    transcomEventIdsIter,
    batchSize,
    sleepMs
  );

  let count = 0;

  for await (const event of iter) {
    ++count;

    let ready = gzip.write(`${JSON.stringify(event)}\n`);

    if (!ready) {
      await new Promise((resolve) => gzip.once("drain", resolve));
    }
  }

  gzip.end();

  console.log(`"${filename}":`, count);

  await done;
}

export async function loadApiScrapeDirectoryIntoDatabase(
  apiScrapeDir: string,
  schemaName: string,
  tableName: string,
  db: Client
) {
  const rawEventsIter =
    makeRawTranscomEventsExpandedIteratorFromApiScrapeDirectory(apiScrapeDir);

  const protoTranscomEventIter =
    transformRawTranscomEventExpandedIteratorToProtoTranscomEventExpandedIterator(
      rawEventsIter
    );

  await loadProtoTranscomEventsExpandedIntoDatabase(
    protoTranscomEventIter,
    schemaName,
    tableName,
    db
  );
}
