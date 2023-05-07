import {
  writeFile as writeFileAsync,
  readFile as readFileAsync,
} from "fs/promises";

import { join } from "path";
import { NpmrdsTmc } from "../domain";

import getEtlMetadataDir from "./getEtlMetadataDir";

export function getTmcsListFilePath() {
  const etl_metadata_dir = getEtlMetadataDir();
  return join(etl_metadata_dir, "tmcs.json");
}

export async function setTmcs(tmcs: NpmrdsTmc[]) {
  const fpath = getTmcsListFilePath();

  await writeFileAsync(fpath, JSON.stringify(tmcs, null, 4));
}

export async function getTmcs() {
  const fpath = getTmcsListFilePath();

  const stringified = await readFileAsync(fpath, { encoding: "utf8" });

  const tmcs = JSON.parse(stringified);

  return tmcs;
}
