import { mkdirSync } from "fs";
import { join } from "path";

import getEtlWorkDir from "var/getEtlWorkDir";

export default function getEtlMetadataDir() {
  const etl_work_dir = getEtlWorkDir();

  const dir = join(etl_work_dir, "metadata");

  mkdirSync(dir, { recursive: true });

  return dir;
}
