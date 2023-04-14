import { v4 as uuid } from "uuid";
import pgFormat from "pg-format";
import _ from "lodash";

import dama_db from ".";

const PG_ENV = "ephemeral_test_db";

const getRandomTableName = () =>
  uuid()
    .replace(/[^0-9a-z]/gi, "")
    .replace(/^[0-9]+/, "x");

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

test("default transaction contexts exhibit READ COMMITTED ISOLATION", async () => {
  const table_name = getRandomTableName();

  const sql = pgFormat("CREATE TABLE %I ( i INTEGER );", table_name);

  await dama_db.query(sql, PG_ENV);

  // Needs to be less than the number of Pool clients available.
  const n = 5;
  const task_ids = _.range(0, n);

  const task_done_fns: Function[] = [];

  const all_done = Promise.all(
    task_ids.map((i) => new Promise((resolve) => (task_done_fns[i] = resolve)))
  );

  // Because no task COMMITs until all have INSERTed their data, no task sees other's INSERTs.
  const task_fn = async (task_id: number) => {
    await dama_db.query({
      text: pgFormat(
        `
          INSERT INTO %I ( i )
            SELECT
                i
              FROM generate_series(1, $1) AS t(i)
          ;
        `,
        table_name
      ),
      values: [n],
    });

    const {
      rows: [{ count_in_ctx }],
    } = await dama_db.query(
      pgFormat(
        `
          SELECT
              COUNT(1)::INTEGER AS count_in_ctx
            FROM %I
          ;
        `,
        table_name
      )
    );

    expect(count_in_ctx).toBe(n);

    task_done_fns[task_id]();

    // None COMMIT until all COMMIT since function does not return.
    await all_done;
  };

  await Promise.all(
    task_ids.map((task_id) =>
      dama_db.runInTransactionContext(task_fn.bind(null, task_id), PG_ENV)
    )
  );

  const {
    rows: [{ total_count }],
  } = await dama_db.query(
    pgFormat(
      `
        SELECT
            COUNT(1)::INTEGER AS total_count
          FROM %I
      `,
      table_name
    ),
    PG_ENV
  );

  expect(total_count).toBe(n ** 2);
});

// This one doesn't always pass.
test.skip("demonstrate transaction contexts permits PHANTOM READs", async () => {
  const table_name = getRandomTableName();

  const sql = pgFormat(
    `
      CREATE TABLE %I (
        task_id INTEGER, 
        i INTEGER
      );
    `,
    table_name
  );

  await dama_db.query(sql, PG_ENV);

  // Needs to be less than the number of Pool clients available.
  const n = 5;
  const task_ids = _.range(0, n);

  const task_started_fns: Function[] = [];

  const all_ready = Promise.all(
    task_ids.map(
      (i) => new Promise((resolve) => (task_started_fns[i] = resolve))
    )
  );

  // Because no task BEGINs until all have begun, no task sees other's INSERTs.
  const task_fn = async (task_id: number) => {
    const others_ct_sql = pgFormat(
      `
          SELECT
              COUNT(1)::INTEGER AS others_ct
            FROM %I
            WHERE ( task_id <> $1 )
          ;
        `,
      table_name
    );

    const {
      rows: [{ others_ct: initial_others_ct }],
    } = await dama_db.query({ text: others_ct_sql, values: [task_id] });

    expect(initial_others_ct).toBe(0);

    task_started_fns[task_id]();

    await all_ready;

    await dama_db.query({
      text: pgFormat(
        `
          INSERT INTO %I ( task_id, i )
            SELECT
                $1 AS task_id,
                i
              FROM generate_series(1, $2) AS t(i)
          ;
        `,
        table_name
      ),
      values: [task_id, n],
    });

    const {
      rows: [{ others_ct: subsequent_others_ct }],
    } = await dama_db.query({ text: others_ct_sql, values: [task_id] });

    // PHANTOM READ
    return initial_others_ct === subsequent_others_ct;
  };

  const all_tasks_results = await Promise.all(
    task_ids.map((task_id) =>
      dama_db.runInTransactionContext(task_fn.bind(null, task_id), PG_ENV)
    )
  );

  expect(all_tasks_results.some((res) => res === false)).toBe(true);
});
