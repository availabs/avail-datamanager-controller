import { join } from "path";

import BetterSQLite, { Database as SQLiteDB } from "better-sqlite3";

import sqlite_db_fname from "../../constants/sqlite_db_fname";
import getEtlWorkDir from "./getEtlWorkDir";

const cache: Record<string, WeakRef<SQLiteDB>> = {};

export default function getEtlContextLocalStateSqliteDb() {
  const etl_work_dir = getEtlWorkDir();

  const db_ref = cache[etl_work_dir];

  let db = db_ref.deref();

  if (db) {
    return db;
  }

  db = new BetterSQLite(join(etl_work_dir, sqlite_db_fname));

  cache[etl_work_dir] = new WeakRef(db);

  return db;
}
