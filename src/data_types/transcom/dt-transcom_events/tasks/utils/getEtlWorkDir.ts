import { existsSync, mkdirSync } from "fs";
import { join } from "path";

import etl_dir from "constants/etlDir";

import { getPgEnv, getEtlContextId } from "data_manager/contexts";

export default function getEtlWorkDir(
  pg_env = getPgEnv(),
  etl_context_id = getEtlContextId()
) {
  const etl_work_dir = join(
    etl_dir,
    `transcom.download_transcom_events.pg_env_${pg_env}.etl_context_${etl_context_id}`
  );

  if (!existsSync(etl_work_dir)) {
    mkdirSync(etl_work_dir, { recursive: true });
  }

  return etl_work_dir;
}
