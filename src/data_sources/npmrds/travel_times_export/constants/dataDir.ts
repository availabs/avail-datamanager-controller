import { mkdirSync } from "fs";
import { join } from "path";

import dataDir from "../../constants/dataDir";

const dir = join(dataDir, "npmrds_travel_times_export");

mkdirSync(dir, { recursive: true });

export default dir;
