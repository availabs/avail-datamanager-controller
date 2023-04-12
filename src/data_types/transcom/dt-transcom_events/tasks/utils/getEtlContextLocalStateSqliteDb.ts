import { join } from "path";

import BetterSQLite, { Database as SQLiteDB } from "better-sqlite3";

import { getEtlWorkDirMeta } from "./etlWorkDir";

import getPostgresStagingSchemaName from "./getPostgresStagingSchemaName";

const cache: Record<string, WeakRef<SQLiteDB>> = {};

export const sqlite_db_name = "etl_context_local_state.sqlite3";

export const getSqliteDbPath = (etl_work_dir: string) =>
  join(etl_work_dir, sqlite_db_name);

export default function getEtlContextLocalStateSqliteDb(etl_work_dir: string) {
  const db_ref = cache[etl_work_dir];

  let db = db_ref?.deref();

  if (db) {
    return db;
  }

  const sqlite_db_path = getSqliteDbPath(etl_work_dir);

  db = new BetterSQLite(sqlite_db_path);

  cache[etl_work_dir] = new WeakRef(db);

  const { pg_env, etl_context_id } = getEtlWorkDirMeta(etl_work_dir);

  const staging_schema = getPostgresStagingSchemaName(etl_context_id);

  // For event-level IDEMPOTENCY
  db.exec(`
    BEGIN ;

    CREATE TABLE IF NOT EXISTS etl_context
      AS
        SELECT
            '${pg_env}' AS pg_env,
            CAST(${etl_context_id} AS INTEGER) AS etl_context_id,
            '${staging_schema}' AS staging_schema
    ;

    CREATE TABLE IF NOT EXISTS seen_event (
      event_id TEXT PRIMARY KEY
    ) WITHOUT ROWID ;

    CREATE TABLE IF NOT EXISTS downloaded_event (
      event_id  TEXT PRIMARY KEY,
      file_name TEXT NOT NULL
    ) WITHOUT ROWID ;

    COMMIT ;
  `);

  return db;
}
