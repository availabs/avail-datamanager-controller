#!/usr/bin/env node

import {
  readdirSync,
  createReadStream,
  createWriteStream,
  mkdirSync,
} from "fs";
import { createGunzip, createGzip } from "zlib";
import { join } from "path";
import { pipeline } from "stream";
import split from "split2";

import _ from "lodash";

import { getNowTimestamp } from "../../utils/dates";

import { makeRawTranscomEventsExpandedIterator } from "../transcomEventsExtendedDataUtils";

const DATA_YEAR = process.argv[2] || "\\d\\d\\d\\d";

const rawEventsDir = join(__dirname, "./raw-transcom-events-scrape.20220519/");

const expandedEventsOutputDir = join(
  __dirname,
  `raw-transcom-events-expanded-scrape.${getNowTimestamp()}`
);

mkdirSync(expandedEventsOutputDir, { recursive: true });

function getExpandedEventsOutputFileName(year: number) {
  return `raw-transcom-events-expanded.${year}.${getNowTimestamp()}.ndjson.gz`;
}

const getYearFromRawEventsFileName = (f: string) => +f.slice(20, 24);

async function main() {
  const rawEventFileBatches = readdirSync(rawEventsDir)
    .filter((f) =>
      new RegExp(`^raw-transcom-events.${DATA_YEAR}.*\.ndjson.gz$`).test(f)
    )
    .filter((f) => {
      const year = getYearFromRawEventsFileName(f);

      return year <= 2020 && year >= 2019;
    })
    .sort()
    .reverse()
    .reduce((acc: string[][], f, i) => {
      if (i === 0) {
        acc.push([f]);
      } else {
        const prevYear = getYearFromRawEventsFileName(_.last(_.last(acc)));
        const curYear = getYearFromRawEventsFileName(f);

        if (prevYear !== curYear) {
          acc.push([f]);
        } else {
          _.last(acc).push(f);
        }
      }

      return acc;
    }, []);

  const seenEventIds: Set<string> = new Set();

  for (const rawEventFilesForYear of rawEventFileBatches) {
    const yearEventIds: Set<string> = new Set();
    const year = getYearFromRawEventsFileName(rawEventFilesForYear[0]);

    for (const rawEventFile of rawEventFilesForYear) {
      const rawEventPath = join(rawEventsDir, rawEventFile);

      const rawEventsIter = pipeline(
        createReadStream(rawEventPath),
        createGunzip(),
        split(JSON.parse),
        (err) => {
          if (err) {
            console.error(err);
          }
        }
      );

      for await (const { id } of rawEventsIter) {
        if (id) {
          if (!seenEventIds.has(id)) {
            yearEventIds.add(id);
            seenEventIds.add(id);
          }
        }
      }
    }

    console.log("num yearEventIds:", yearEventIds.size);

    try {
      const timerId = `download ${year} events expanded data`;
      console.time(timerId);
      const filename = getExpandedEventsOutputFileName(year);

      const filepath = join(expandedEventsOutputDir, filename);

      const ws = createWriteStream(filepath);
      const gzip = createGzip();

      const done = new Promise((resolve, reject) => {
        ws.once("error", reject);
        ws.once("finish", resolve);
      });

      gzip.pipe(ws);

      const iter = makeRawTranscomEventsExpandedIterator(yearEventIds);

      let count = 0;

      for await (const event of iter) {
        ++count;

        let ready = gzip.write(`${JSON.stringify(event)}\n`);

        if (!ready) {
          await new Promise<void>((resolve) => gzip.once("drain", resolve));
        }
      }

      gzip.end();

      console.log(`"${filename}":`, count);

      await done;
      console.timeEnd(timerId);
    } catch (err) {
      console.error("ERROR downloading", year);
      console.error(err);
    }
  }
}

main();
