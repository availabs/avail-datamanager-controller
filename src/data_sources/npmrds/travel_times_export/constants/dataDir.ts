import { mkdirSync } from "fs";
import { join } from "path";

import dataDir from "../../constants/dataDir";

console.log("\n\n\n==> dataDir", dataDir);

const dir = join(dataDir, "npmrds_travel_times_export");

mkdirSync(dir, { recursive: true });

export default dir;
