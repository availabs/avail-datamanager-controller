import { mkdirSync } from "fs";
import { join } from "path";

import etl_dir from "constants/etlDir";

import {
  EtlWorkDir,
  getPgEnv,
  getEtlContextId,
  getEtlWorkDir as getEtlWorkDirFromContext,
} from "data_manager/contexts";

export default function getEtlWorkDir(
  etl_context_id = getEtlContextId(),
  pg_env = getPgEnv(),
  mkdir_if_dne = true
) {
  let etl_work_dir: EtlWorkDir | null = null;

  try {
    etl_work_dir = getEtlWorkDirFromContext();
  } catch (err) {
    //
  }

  if (!etl_work_dir) {
    const dirname = `etl-pg_env-${pg_env}-eci-${etl_context_id}`;

    etl_work_dir = join(etl_dir, dirname);
  }

  if (mkdir_if_dne) {
    mkdirSync(etl_work_dir, { recursive: true });
  }

  return etl_work_dir;
}
