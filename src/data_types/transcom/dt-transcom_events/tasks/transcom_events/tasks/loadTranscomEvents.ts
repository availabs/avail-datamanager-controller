#!/usr/bin/env node

require("ts-node").register();

import pgFormat from "pg-format";

import { PgEnv } from "../../../domain/PostgreSQLTypes";
import { loadApiScrapeDirectoryIntoDatabase } from "..";

import { getConnectedPgClient } from "../../../utils/PostgreSQL";

export default async function loadTranscomEventsScape(
  pg_env: PgEnv,
  apiScrapeDir: string,
  tableSchema: string,
  tableName: string
) {
  const db = await getConnectedPgClient(pg_env);

  try {
    await db.query("BEGIN;");

    const sql = pgFormat(
      `
        CREATE TABLE IF NOT EXISTS %I.%I (
          LIKE _transcom_admin.transcom_events
            INCLUDING DEFAULTS
            EXCLUDING CONSTRAINTS
        ) ;
      `,
      tableSchema,
      tableName
    );

    await db.query(sql);

    await loadApiScrapeDirectoryIntoDatabase(
      apiScrapeDir,
      tableSchema,
      tableName,
      db
    );

    await db.query("COMMIT;");
  } catch (err) {
    await db.query("ROLLBACK;");
    console.error(err);
  } finally {
    await db.end();
  }
}
