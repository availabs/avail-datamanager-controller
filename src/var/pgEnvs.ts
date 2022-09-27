import { readdirSync } from "fs";
import { join } from "path";

import chokidar from "chokidar";

const configDir = join(__dirname, "../../config");

function getAvailablePgEnvs() {
  return <string[]>readdirSync(configDir)
    .map((f) => {
      if (!/^postgres\..*\.env$/.test(f)) {
        return null;
      }

      return f.replace(/^postgres\./, "").replace(/\.env$/, "");
    })
    .filter(Boolean);
}

const pgEnvs: string[] = [];

function updateAvailablePgEnvs() {
  const envs = getAvailablePgEnvs();

  pgEnvs.length = 0;
  pgEnvs.push(...envs);

  console.log(JSON.stringify({ pgEnvs }, null, 4));
}

// https://github.com/paulmillr/chokidar
chokidar
  .watch(configDir, { ignoreInitial: true })
  .on("add", updateAvailablePgEnvs)
  .on("unlink", updateAvailablePgEnvs);

updateAvailablePgEnvs();

export default pgEnvs;
