import { mkdirSync } from "fs";
import { join } from "path";

import ETL_CONTEXT_ID from "./etl_context_id";

const LOGS_DIR = join(__dirname, "../../../../../task_logs");

mkdirSync(LOGS_DIR, { recursive: true });

const LOG_FILE_PATH = join(
  LOGS_DIR,
  `task.etl_context_id.${ETL_CONTEXT_ID}.log`
);

export default LOG_FILE_PATH;
