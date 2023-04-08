import { Client } from "pg";
import { v4 as uuid } from "uuid";
import pgFormat from "pg-format";
import dedent from "dedent";

import dama_db from ".";
import { getNodePgCredentials } from "./postgres/PostgreSQL";

const PG_ENV = "test_db";

beforeAll(async () => {
  const creds = getNodePgCredentials(PG_ENV);

  if (creds.host !== "127.0.0.1") {
    throw new Error("test_db host MUST = 127.0.0.1");
  }

  if (creds.host !== "127.0.0.1") {
    throw new Error("test_db user MUST = dama_test_user");
  }

  creds.database = "postgres";

  const db = new Client(creds);

  try {
    await db.connect();

    await db.query("DROP DATABASE IF EXISTS test_db;");
    await db.query("CREATE DATABASE test_db;");
  } catch (err) {
    throw err;
  } finally {
    await db.end();
  }
});

afterAll(async () => {
  await dama_db.shutdown();
});

test("initializes data_manager schema on connect", async () => {
  const {
    rows: [{ data_manager_schema_exists }],
  } = await dama_db.query(
    `
      SELECT EXISTS (
        SELECT
            1
          FROM pg_catalog.pg_namespace
          WHERE nspname = 'data_manager'
      ) AS data_manager_schema_exists
    `,
    PG_ENV
  );

  expect(data_manager_schema_exists).toBe(true);
});

test("transaction context commits", async () => {
  const table_name = uuid().replace(/[^0-9a-z]/gi, "");

  await dama_db.runInTransactionContext(async () => {
    const sql = pgFormat("CREATE TABLE %I ( id INTEGER );", table_name);

    await dama_db.query(sql);
  }, PG_ENV);

  const {
    rows: [{ table_exists }],
  } = await dama_db.query(
    pgFormat(
      `
        SELECT EXISTS (
          SELECT
              1
            FROM pg_catalog.pg_tables
            WHERE ( tablename = %L )
        ) AS table_exists
      `,
      table_name
    ),
    PG_ENV
  );

  expect(table_exists).toBe(true);
});

test("transaction context rolls back on error", async () => {
  const table_name = uuid().replace(/[^0-9a-z]/gi, "");

  try {
    await dama_db.runInTransactionContext(async () => {
      const sql = pgFormat("CREATE TABLE %I ( id INTEGER );", table_name);

      await dama_db.query(sql);

      throw new Error();
    }, PG_ENV);
  } catch (err) {
    //
  }

  const {
    rows: [{ table_exists }],
  } = await dama_db.query(
    pgFormat(
      `
        SELECT EXISTS (
          SELECT
              1
            FROM pg_catalog.pg_tables
            WHERE ( tablename = %L )
        ) AS table_exists
      `,
      table_name
    ),
    PG_ENV
  );

  expect(table_exists).toBe(false);
});
