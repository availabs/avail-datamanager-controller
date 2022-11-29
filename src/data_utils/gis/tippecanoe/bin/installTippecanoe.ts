import { exec } from "child_process";
import { existsSync } from "fs";
import { rm as rmAsync, mkdir as mkdirAsync } from "fs/promises";
import { basename } from "path";
import { promisify } from "util";

import dedent from "dedent";

import tippecanoePath, { tippecanoeDir } from "../constants/tippecanoePath";

const execAsync = promisify(exec);

export default async function installTippecanoe(clean = false) {
  console.log("installTippecanoe");
  if (existsSync(tippecanoePath)) {
    if (clean) {
      await rmAsync(tippecanoeDir, { recursive: true, force: true });
    }
    return;
  }

  await rmAsync(tippecanoeDir, { recursive: true, force: true });
  await mkdirAsync(tippecanoeDir, { recursive: true });

  console.log("installing tippecanoe in", tippecanoeDir);

  const { stdout, stderr } = await execAsync(
    dedent(`
      git clone https://github.com/mapbox/tippecanoe.git --branch=1.36.0
      cd tippecanoe
      make -j
    `),
    {
      cwd: basename(tippecanoeDir),
    }
  );

  console.log("tippecanoe install stdout:");
  console.log(stdout.toString());
  console.error("tippecanoe install stderr:");
  console.error(stderr.toString());
}
