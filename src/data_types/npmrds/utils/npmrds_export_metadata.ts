import { readFileSync, existsSync } from "fs";
import { writeFile as writeFileAsync } from "fs/promises";

import { join } from "path";

import getEtlWorkDir from "var/getEtlWorkDir";

import { NpmrdsExportMetadata } from "../domain";

export const getNpmrdsExportMetadataFilePath = () =>
  join(getEtlWorkDir(), "npmrds_export_request.json");

export async function setNpmrdsExportMetadataAsync(
  npmrds_export_metadata: NpmrdsExportMetadata
) {
  const fpath = getNpmrdsExportMetadataFilePath();

  return writeFileAsync(fpath, JSON.stringify(npmrds_export_metadata, null, 4));
}

export function getNpmrdsExportMetadata(): NpmrdsExportMetadata {
  const fpath = getNpmrdsExportMetadataFilePath();

  if (!existsSync(fpath)) {
    throw new Error(`NpmrdsExportMetadata file does not exist: ${fpath}`);
  }

  const str = readFileSync(fpath, { encoding: "utf8" });

  return JSON.parse(str);
}
