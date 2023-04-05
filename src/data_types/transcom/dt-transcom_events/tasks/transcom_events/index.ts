import {
  readdirSync,
  createReadStream,
  createWriteStream,
  mkdirSync,
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

import {
  validateTranscomRequestTimestamp,
  partitionTranscomRequestTimestampsByMonth,
  getNowTimestamp,
  TranscomApiRequestTimestamp,
} from "../utils/dates";

import { RawTranscomEvent, ProtoTranscomEvent } from "./index.d";

import { url, apiResponsePropsToDbCols, dbCols } from "./data_schema";

const pipelineAsync = promisify(pipeline);

const DEFAULT_SLEEP_MS = 10 * 1000; // 10 seconds

function getRawTranscomEventsFileName(
  startTimestamp: TranscomApiRequestTimestamp,
  endTimestamp: TranscomApiRequestTimestamp
) {
  const start = startTimestamp.replace(/-|:/g, "").replace(/ /, "T");
  const end = endTimestamp.replace(/-|:/g, "").replace(/ /, "T");

  return `raw-transcom-events.${start}-${end}.${getNowTimestamp()}.ndjson.gz`;
}

function getAuthenticationToken() {
  const p = join(__dirname, "./config/authenticationtoken");
  const t = readFileSync(p, { encoding: "utf8" }).trim();

  return t;
}

export async function* makeRawTranscomEventIterator(
  startTimestamp: string,
  endTimestamp: string,
  sleepMs: number = DEFAULT_SLEEP_MS
): AsyncGenerator<RawTranscomEvent> {
  validateTranscomRequestTimestamp(startTimestamp);
  validateTranscomRequestTimestamp(endTimestamp);

  const partitionedDateTimes = partitionTranscomRequestTimestampsByMonth(
    startTimestamp,
    endTimestamp
  );

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

    const authenticationtoken = getAuthenticationToken();

    console.log("authenticationtoken:", authenticationtoken);

    const options = {
      searchParams: {
        userId: 78,
      },

      headers: {
        authenticationtoken,
      },

      json: reqBody,
    };

    console.log("makeRawTranscomEventIterator: start");
    const { data: events } = await got.post(url, options).json();
    console.log("makeRawTranscomEventIterator: done");

    if (Array.isArray(events)) {
      for (const event of events) {
        yield event;
      }
    }

    await new Promise((resolve) => setTimeout(resolve, sleepMs));
  }
}

export function transformRawTranscomEventToProtoTranscomEvent(
  rawTranscomEvent: RawTranscomEvent
) {
  const d = _(rawTranscomEvent)
    .mapKeys((_v, k) => apiResponsePropsToDbCols[k])
    .mapValues((v) => (v === "" ? null : v))
    .value();

  // @ts-ignore
  d.event_type = d.event_type?.toLowerCase();
  // @ts-ignore
  d.direction = d.direction?.toLowerCase();
  // @ts-ignore
  d.event_status = d.event_status?.toLowerCase();

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
    (err) => {
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
  tableName: string,
  db: Client
) {
  const rawEventIter =
    makeRawTranscomEventIteratorFromApiScrapeDirectory(apiScrapeDir);

  const protoTranscomEventIter =
    transformRawTranscomEventIteratorToProtoTranscomEventIterator(rawEventIter);

  await loadProtoTranscomEventsIntoDatabase(
    protoTranscomEventIter,
    schemaName,
    tableName,
    db
  );
}

export async function downloadTranscomEvents(
  startTimestamp: TranscomApiRequestTimestamp,
  endTimestamp: TranscomApiRequestTimestamp,
  outputDir: string
) {
  console.log(
    JSON.stringify({ startTimestamp, endTimestamp, outputDir }, null, 4)
  );
  mkdirSync(outputDir, { recursive: true });

  const monthPartitions = partitionTranscomRequestTimestampsByMonth(
    startTimestamp,
    endTimestamp
  );

  for (const [monthStartTimestamp, monthEndTimestamp] of monthPartitions) {
    const filename = getRawTranscomEventsFileName(
      monthStartTimestamp,
      monthEndTimestamp
    );

    try {
      const filepath = join(outputDir, filename);

      const ws = createWriteStream(filepath);
      const gzip = createGzip();

      const done = new Promise((resolve, reject) => {
        ws.once("error", reject);
        ws.once("finish", resolve);
      });

      gzip.pipe(ws);

      const iter = makeRawTranscomEventIterator(
        monthStartTimestamp,
        monthEndTimestamp
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
    } catch (err) {
      console.error("ERROR downloading", filename);
      console.error(err);
    }
  }
}
