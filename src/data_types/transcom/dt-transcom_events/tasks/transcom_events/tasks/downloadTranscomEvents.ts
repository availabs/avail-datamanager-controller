#!/usr/bin/env node

/* eslint-disable no-restricted-syntax */

import { createWriteStream, mkdirSync } from "fs";
import { createGzip } from "zlib";
import { join } from "path";

import _ from "lodash";

import {
  partitionTranscomRequestTimestampsByMonth,
  getNowTimestamp,
  TranscomApiRequestTimestamp,
} from "../../utils/dates";

import { makeRawTranscomEventIterator } from "..";

function getRawTranscomEventsFileName(
  startTimestamp: TranscomApiRequestTimestamp,
  endTimestamp: TranscomApiRequestTimestamp
) {
  const start = startTimestamp.replace(/-|:/g, "").replace(/ /, "T");
  const end = endTimestamp.replace(/-|:/g, "").replace(/ /, "T");

  return `raw-transcom-events.${start}-${end}.${getNowTimestamp()}.ndjson.gz`;
}

export default async function downloadTranscomEvents(
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
