import { mkdirSync } from "fs";
import { join, basename } from "path";

import { getPgEnv, getEtlContextId } from "data_manager/contexts";
import etl_dir from "constants/etlDir";

import {
  getSqliteDbPath,
  sqlite_db_name,
} from "../utils/getEtlContextLocalStateSqliteDb";

const raw_transcom_events_download_dir_name = "raw-transcom-events-expanded";

export function getRawTranscomEventsDownloadDir(etl_work_dir: string) {
  const events_dir = join(etl_work_dir, raw_transcom_events_download_dir_name);

  mkdirSync(events_dir, { recursive: true });

  return events_dir;
}

export function getRawTranscomEventsFilePath(
  etl_work_dir: string,
  file_name: string
) {
  const events_dir = getRawTranscomEventsDownloadDir(etl_work_dir);

  return join(events_dir, file_name);
}

export function getEtlWorkDirBasename(pg_env: string, etl_context_id: number) {
  return `dt-transcom_events.${pg_env}.${etl_context_id}`;
}

export function getEtlWorkDirMeta(etl_work_dir: string) {
  const dir_name = basename(etl_work_dir);

  const [, pg_env, etl_context_id_str] = dir_name.split(".");

  const etl_context_id = +etl_context_id_str;

  return {
    pg_env,
    etl_context_id,

    sqlite_db_name,
    sqlite_db_path: getSqliteDbPath(etl_work_dir),

    raw_transcom_events_download_dir_name,
    raw_transcom_events_download_dir_path:
      getRawTranscomEventsDownloadDir(etl_work_dir),
  };
}

export default function getEtlWorkDir(
  pg_env = getPgEnv(),
  etl_context_id = getEtlContextId(),
  parent_dir = etl_dir // So tests can use tmp dir.
) {
  if (!Number.isFinite(etl_context_id)) {
    throw new Error(`invalid etl_context_id ${etl_context_id}`);
  }

  const etl_work_dir = join(
    parent_dir,
    getEtlWorkDirBasename(pg_env, <number>etl_context_id)
  );

  mkdirSync(etl_work_dir, { recursive: true });

  getRawTranscomEventsDownloadDir(etl_work_dir);

  return etl_work_dir;
}
