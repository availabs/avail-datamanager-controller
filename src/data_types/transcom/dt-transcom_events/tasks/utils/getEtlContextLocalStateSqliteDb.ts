import { join } from "path";

import BetterSQLite, { Database as SQLiteDB } from "better-sqlite3";

const cache: Record<string, WeakRef<SQLiteDB>> = {};

const sqlite_db_fname = "etl_context_local_state.sqlite3";

export default function getEtlContextLocalStateSqliteDb(etl_work_dir: string) {
  const db_ref = cache[etl_work_dir];

  let db = db_ref?.deref();

  if (db) {
    return db;
  }

  db = new BetterSQLite(join(etl_work_dir, sqlite_db_fname));

  cache[etl_work_dir] = new WeakRef(db);

  // For event-level IDEMPOTENCY
  db.exec(`
    BEGIN ;

    CREATE TABLE IF NOT EXISTS seen_event (
      event_id TEXT PRIMARY KEY
    ) WITHOUT ROWID ;

    CREATE TABLE IF NOT EXISTS downloaded_event (
      event_id  TEXT PRIMARY KEY,
      file_path TEXT NOT NULL
    ) WITHOUT ROWID ;

    COMMIT ;
  `);

  return db;
}
