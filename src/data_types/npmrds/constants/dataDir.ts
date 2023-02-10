import { mkdirSync } from "fs";
import { join } from "path";

import dataDir from "../../../constants/dataDir";

const dir = join(dataDir, "data_sources/npmrds/");

mkdirSync(dir, { recursive: true });

export default dir;
