//  https://github.com/CacheControl/json-rules-engine
//    A rules engine expressed in JSON
//  Could be used to populate the Features' tippecanoe object
//    https://github.com/mapbox/tippecanoe#geojson-extension

import { spawn } from "child_process";
import { existsSync, createWriteStream } from "fs";
import { pipeline } from "stream";
import { promisify } from "util";
import { createGzip } from "zlib";
import { basename } from "path";

import tmp from "tmp";

import { Feature } from "geojson";

import tippecanoePath from "../../../../../data_utils/gis/tippecanoe/constants/tippecanoePath";
import installTippecanoe from "../../../../../data_utils/gis/tippecanoe/bin/installTippecanoe";

import etlDir from "../../../../../constants/etlDir";

import asyncGeneratorToNdjsonStream from "../../../../../data_utils/streaming/asyncGeneratorToNdjsonStream";

const pipelineAsync = promisify(pipeline);

export type CreateMBTilesConfig = {
  layerName: string;
  mbtilesFilePath: string;
  featuresAsyncIterator: AsyncGenerator<Feature>;
  etlWorkDir?: string;
};

export default async function main({
  layerName,
  mbtilesFilePath,
  featuresAsyncIterator,
  etlWorkDir,
}: CreateMBTilesConfig) {
  if (!etlWorkDir) {
    etlWorkDir = await new Promise((resolve, reject) =>
      tmp.dir({ tmpdir: etlDir }, (err, path) => {
        if (err) {
          return reject(err);
        }

        resolve(path);
      })
    );
  }

  const {
    path: geojsonFilePath,
    fd: geojsonFileDescriptor,
    cleanupCallback: geojsonFileCleanup,
  } = await new Promise((resolve, reject) =>
    tmp.file(
      { tmpdir: etlWorkDir, postfix: ".geojson.gz" },
      (err, path, fd, cleanupCallback) => {
        if (err) {
          return reject(err);
        }

        resolve({ path, fd, cleanupCallback });
      }
    )
  );

  // @ts-ignore
  const ws = createWriteStream(null, {
    fd: geojsonFileDescriptor,
  });

  const gzip = createGzip({ level: 9 });
  await pipelineAsync(
    asyncGeneratorToNdjsonStream(featuresAsyncIterator),
    gzip,
    ws
  );

  let success: Function;
  let fail: Function;

  if (!existsSync(tippecanoePath)) {
    console.log("Installing tippecanoe...");
    await installTippecanoe();
  }

  const done = new Promise((resolve, reject) => {
    success = resolve;
    fail = reject;
  });

  const name = basename(mbtilesFilePath, ".mbtiles");

  const tippecanoeArgs = [
    "--no-progress-indicator",
    "--read-parallel",
    "--no-feature-limit",
    "--no-tile-size-limit",
    "--generate-ids",
    "-r1",
    "--force",
    "--name",
    name,
    "--layer",
    layerName,
    "-o",
    mbtilesFilePath,
    geojsonFilePath,
  ];

  const tippecanoeCProc = spawn(tippecanoePath, tippecanoeArgs, {
    stdio: "pipe",
  })
    // @ts-ignore
    .once("error", fail)
    // @ts-ignore
    .once("close", success);

  let tippecanoeStdout = "";

  /* eslint-disable-next-line no-unused-expressions */
  tippecanoeCProc.stdout?.on("data", (data) => {
    // process.stdout.write(data);
    tippecanoeStdout += data.toString();
  });

  let tippecanoeStderr = "";

  /* eslint-disable-next-line no-unused-expressions */
  tippecanoeCProc.stderr?.on("data", (data) => {
    // process.stderr.write(data);
    tippecanoeStderr += data.toString();
  });

  await done;

  await geojsonFileCleanup();

  return {
    layerName,
    mbtilesFilePath,
    geojsonFilePath,
    tippecanoeArgs,
    tippecanoeStdout,
    tippecanoeStderr,
  };
}
