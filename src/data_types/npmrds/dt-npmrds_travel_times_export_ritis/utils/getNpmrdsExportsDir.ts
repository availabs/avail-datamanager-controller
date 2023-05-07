import { mkdirSync } from "fs";
import { join } from "path";

import getEtlWorkDir from "var/getEtlWorkDir";

export default function getNpmrdsExportsDir() {
  const dir = join(getEtlWorkDir(), "npmrds_exports");

  mkdirSync(dir, { recursive: true });

  return dir;
}
