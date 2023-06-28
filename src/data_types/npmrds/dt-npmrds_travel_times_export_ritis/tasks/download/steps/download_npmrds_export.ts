import { createWriteStream, mkdirSync } from "fs";
import { pipeline } from "stream";
import { join } from "path";
import { promisify } from "util";

import fetch from "node-fetch";

import dama_events from "data_manager/events";
import logger from "data_manager/logger";
import { verifyIsInTaskEtlContext } from "data_manager/contexts";

import { sleep } from "data_utils/time";

import { makeFileReadOnlyAsync } from "data_utils/files";
import {
  NpmrdsExportReadyMeta,
  NpmrdsExportDownloadPaths,
  NpmrdsExportDownloadMeta,
} from "../../../domain";

import getNpmrdsExportsDir from "../../../utils/getNpmrdsExportsDir";

import { NpmrdsExportReadyForDownloadEventType } from "../../../puppeteer/MyHistory";

export const NpmrdsExportDownloadedType = ":NPMRDS_EXPORT_DOWNLOADED";

const pipelineAsync = promisify(pipeline);

const THIRTY_SECONDS = 1000 * 30;

// So in testing, we can mock this out.
export async function downloadFile(fpath: string, url: string) {
  //  necessary modifications to the url:
  //    * http => https
  //    * add the ?dl=1 query param
  if (/^http:/.test(url)) {
    url = url.replace(/http:/, "https:");
  }

  if (!/\?dl=1$/.test(url)) {
    url = `${url}?dl=1`;
  }

  return new Promise<void>((resolve, reject) => {
    // https://github.com/node-fetch/node-fetch#streams
    const ws = createWriteStream(fpath)
      .on("error", (err) => reject(err))
      .on("ready", async () => {
        const res = await fetch(url);

        if (!res.ok) {
          return reject(
            new Error(
              `NPMRDS Export Download: unexpected response ${res.statusText}`
            )
          );
        }

        await pipelineAsync(res.body, ws);

        resolve();
      });
  });
}

export async function downloadNpmrdsExport(
  npmrds_export_dir: string,
  npmrds_export_ready_meta: NpmrdsExportReadyMeta
): Promise<NpmrdsExportDownloadPaths> {
  const { name, urls } = npmrds_export_ready_meta;

  logger.debug(
    `downloadNpmrdsExport ${JSON.stringify(npmrds_export_ready_meta, null, 4)}`
  );

  let must_sleep = false;

  const npmrds_export_downloads_meta = {};

  for (const [data_source, url] of Object.entries(urls)) {
    if (must_sleep) {
      logger.debug("downloadNpmrdsExport waiting THIRTY_SECONDS");
      await sleep(THIRTY_SECONDS);
    }

    const ds = data_source.toLowerCase();

    const data_src_dir = join(npmrds_export_dir, ds);

    mkdirSync(data_src_dir, { recursive: true });

    const fpath = join(data_src_dir, `${name}.zip`);

    logger.debug(`downloaded ${url} to ${fpath}`);

    await downloadFile(fpath, url);

    await makeFileReadOnlyAsync(fpath);

    npmrds_export_downloads_meta[ds] = fpath;

    must_sleep = true;
  }

  return <NpmrdsExportDownloadPaths>npmrds_export_downloads_meta;
}

export default async function main(): Promise<NpmrdsExportDownloadMeta> {
  verifyIsInTaskEtlContext();

  const events = await dama_events.getAllEtlContextEvents();

  let export_downloaded_event = events.find(
    ({ type }) => type === NpmrdsExportDownloadedType
  );

  if (export_downloaded_event) {
    return export_downloaded_event.payload;
  }

  const export_ready_event = events.find(
    ({ type }) => type === NpmrdsExportReadyForDownloadEventType
  );

  if (!export_ready_event) {
    throw new Error(
      `Did not find a ${NpmrdsExportReadyForDownloadEventType} event in the ETL Context's events`
    );
  }

  const npmrds_export_dir = getNpmrdsExportsDir();

  try {
    const download_paths = await downloadNpmrdsExport(
      npmrds_export_dir,
      export_ready_event.payload
    );

    export_downloaded_event = {
      type: NpmrdsExportDownloadedType,
      payload: {
        name: export_ready_event.payload.name,
        download_paths,
      },
    };

    await dama_events.dispatch(export_downloaded_event);

    return export_downloaded_event.payload;
  } catch (err) {
    console.log("!".repeat(100));
    const { message, stack } = <Error>err;

    logger.error(message);
    logger.error(stack);

    await dama_events.dispatch({
      type: "download_npmrds_export:ERROR",
      payload: {
        message,
        stack,
      },
      error: true,
    });

    throw err;
  }
}
