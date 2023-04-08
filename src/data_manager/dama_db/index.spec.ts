import { Client } from "pg";
import { v4 as uuid } from "uuid";
import pgFormat from "pg-format";

import dama_db from ".";
import { getNodePgCredentials } from "./postgres/PostgreSQL";

const PG_ENV = "test_db";

beforeAll(async () => {
  const creds = getNodePgCredentials(PG_ENV);

  creds.host = "127.0.0.1";
  creds.database = "postgres";
  creds.user = "dama_test_user";

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

const getRandomTableName = () => uuid().replace(/[^0-9a-z]/gi, "");

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

test("single query returns single result", async () => {
  const result = await dama_db.query("SELECT 1 AS foo ;", PG_ENV);

  expect(Array.isArray(result)).toBe(false);

  const {
    rows: [{ foo }],
  } = result;

  expect(foo).toBe(1);
});

test("multi queries returns multi results", async () => {
  const result = await dama_db.query(
    ["SELECT 1 AS foo ;", "SELECT 2 AS bar"],
    PG_ENV
  );

  expect(Array.isArray(result)).toBe(true);

  const [
    {
      rows: [{ foo }],
    },
    {
      rows: [{ bar }],
    },
  ] = result;

  expect(foo).toBe(1);
  expect(bar).toBe(2);
});

test("multi queries returns multi results", async () => {
  const result = await dama_db.query(
    ["SELECT 1 AS foo ;", "SELECT 2 AS bar"],
    PG_ENV
  );

  expect(Array.isArray(result)).toBe(true);

  const [
    {
      rows: [{ foo }],
    },
    {
      rows: [{ bar }],
    },
  ] = result;

  expect(foo).toBe(1);
  expect(bar).toBe(2);
});

test("make async generator from query", async () => {
  const table_name = getRandomTableName();

  await dama_db.query(
    pgFormat(
      `
        CREATE TABLE public.%I
          AS
            SELECT
                n
              FROM generate_series(0, 99) AS t(n)
        ;
      `,
      table_name
    ),
    PG_ENV
  );

  const iter = dama_db.makeIterator(
    pgFormat(
      `
        SELECT
            n
          FROM public.%I
          ORDER BY 1
      `,
      table_name
    ),
    null,
    PG_ENV
  );

  let i = 0;
  for await (const { n } of iter) {
    expect(n).toBe(i++);
  }
});

// ====== dama_db.makeIterator ======

test("async generator works with concurrent queries (no transaction context)", async () => {
  const table_name = getRandomTableName();

  await dama_db.query(
    pgFormat(
      `
        CREATE TABLE public.%I
          AS
            SELECT
                n
              FROM generate_series(0, 99) AS t(n)
        ;
      `,
      table_name
    ),
    PG_ENV
  );

  const iter = dama_db.makeIterator(
    pgFormat(
      `
        SELECT
            n
          FROM public.%I
          ORDER BY 1
      `,
      table_name
    ),
    null,
    PG_ENV
  );

  let i = 0;
  for await (const { n } of iter) {
    expect(n).toBe(i++);

    const {
      rows: [{ num_rows }],
    } = await dama_db.query(
      pgFormat(
        `
          SELECT
              COUNT(1)::INTEGER AS num_rows
            FROM public.%I
        `,
        table_name
      ),
      PG_ENV
    );

    expect(num_rows).toBe(100);
  }
});

// ====== dama_db.runInTransactionContext ======

test("async generator works with concurrent queries (in transaction context)", async () => {
  const table_name = getRandomTableName();

  await dama_db.runInTransactionContext(async () => {
    await dama_db.query(
      pgFormat(
        `
          CREATE TABLE public.%I
            AS 
              SELECT
                  n
                FROM generate_series(0, 99) AS t(n)
          ;
        `,
        table_name
      )
    );

    const iter = dama_db.makeIterator(
      pgFormat(
        `
          SELECT
              n
            FROM public.%I
            ORDER BY 1
        `,
        table_name
      ),
      null
    );

    let i = 0;
    for await (const { n } of iter) {
      expect(dama_db.isInTransactionContext).toBe(true);

      expect(n).toBe(i++);

      const {
        rows: [{ num_rows }],
      } = await dama_db.query(
        pgFormat(
          `
            SELECT
                COUNT(1)::INTEGER AS num_rows
              FROM public.%I
          `,
          table_name
        )
      );

      expect(num_rows).toBe(100);
    }
  }, PG_ENV);

  expect(dama_db.isInTransactionContext).toBe(false);
});

test("transaction context commits when fn returns", async () => {
  const table_name = getRandomTableName();

  const v = await dama_db.runInTransactionContext(async () => {
    expect(dama_db.isInTransactionContext).toBe(true);

    const sql = pgFormat("CREATE TABLE %I ( id INTEGER );", table_name);

    await dama_db.query(sql);

    return "return value";
  }, PG_ENV);

  expect(v).toBe("return value");

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

  expect(dama_db.isInTransactionContext).toBe(false);

  expect(table_exists).toBe(true);
});

test("transaction context rolls back on error", async () => {
  const table_name = getRandomTableName();

  try {
    await dama_db.runInTransactionContext(async () => {
      expect(dama_db.isInTransactionContext).toBe(true);
      const sql = pgFormat("CREATE TABLE %I ( id INTEGER );", table_name);

      await dama_db.query(sql);

      throw new Error("test error");
    }, PG_ENV);
  } catch (err) {
    expect(dama_db.isInTransactionContext).toBe(false);
    expect((<Error>err).message).toBe("test error");
  }

  const {
    rows: [{ table_dne }],
  } = await dama_db.query(
    pgFormat(
      `
        SELECT NOT EXISTS (
          SELECT
              1
            FROM pg_catalog.pg_tables
            WHERE ( tablename = %L )
        ) AS table_dne
      `,
      table_name
    ),
    PG_ENV
  );

  expect(table_dne).toBe(true);
});

test("transaction context rolls back on error", async () => {
  const table_name = getRandomTableName();

  try {
    await dama_db.runInTransactionContext(async () => {
      const sql = pgFormat("CREATE TABLE %I ( id INTEGER );", table_name);

      await dama_db.query(sql);

      throw new Error("test error");
    }, PG_ENV);
  } catch (err) {
    expect((<Error>err).message).toBe("test error");
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

test("transaction contexts exhibit ISOLATION", async () => {
  const table_name = getRandomTableName();

  const sql = pgFormat("CREATE TABLE %I ( msg TEXT );", table_name);

  await dama_db.query(sql, PG_ENV);

  type Config = {
    msg: "a msg" | "b msg";
    done: Function;
  };

  const configs: Record<"a" | "b", Config> = {
    // @ts-ignore
    a: {
      msg: "a msg",
    },
    // @ts-ignore
    b: {
      msg: "b msg",
    },
  };

  const both_done = Promise.all([
    new Promise((resolve) => (configs.a.done = resolve)),
    new Promise((resolve) => (configs.b.done = resolve)),
  ]);

  const task_fn = async (task_id: "a" | "b") => {
    // @ts-ignore
    const { msg, done } = configs[task_id];
    const other_msg = task_id === "a" ? configs.b.msg : configs.a.msg;

    await dama_db.query(
      pgFormat(
        `
          INSERT INTO %I ( msg )
            VALUES ( %L )
        `,
        table_name,
        msg
      )
    );

    const {
      rows: [{ own_msg_exists }],
    } = await dama_db.query(
      pgFormat(
        `
          SELECT EXISTS (
            SELECT
                1
              FROM %I
              WHERE ( msg = %L )
          ) AS own_msg_exists
        `,
        table_name,
        msg
      )
    );

    expect(own_msg_exists).toBe(true);

    const {
      rows: [{ other_msg_dne }],
    } = await dama_db.query(
      pgFormat(
        `
          SELECT NOT EXISTS (
            SELECT
                1
              FROM %I
              WHERE ( msg = %L )
          ) AS other_msg_dne
        `,
        table_name,
        other_msg
      )
    );

    expect(other_msg_dne).toBe(true);

    done();

    await both_done;
  };

  await Promise.all([
    dama_db.runInTransactionContext(task_fn.bind(null, "a"), PG_ENV),
    dama_db.runInTransactionContext(task_fn.bind(null, "b"), PG_ENV),
  ]);

  const { rows } = await dama_db.query(
    pgFormat(
      `
        SELECT
            msg
          FROM %I
          ORDER BY 1
      `,
      table_name
    ),
    PG_ENV
  );

  expect(rows.length).toBe(2);

  const sorted_msgs = Object.keys(configs)
    .map((k) => configs[k].msg)
    .sort();

  for (let i = 0; i < sorted_msgs.length; ++i) {
    expect(rows[i].msg).toBe(sorted_msgs[i]);
  }
});
