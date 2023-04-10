import { v4 as uuid } from "uuid";
import pgFormat from "pg-format";
import _ from "lodash";

import dama_db from "../dama_db";
import dama_meta from ".";

const PG_ENV = "ephemeral_test_db";

const getRandomTableName = () =>
  uuid()
    .replace(/[^0-9a-z]/gi, "")
    .replace(/^[0-9]+/g, "x");

test("describes a data_manager schema table", async () => {
  const table_description = await dama_meta.describeTable(
    "data_manager",
    "sources",
    PG_ENV
  );

  const column_names = Object.keys(table_description);

  expect(column_names.includes("source_id")).toBe(true);
  expect(column_names.includes("name")).toBe(true);
  expect(column_names.includes("type")).toBe(true);
});

test.only("describes DamaView tables", async () => {
  const table_schema = getRandomTableName();
  const table_name = getRandomTableName();

  const ddl = pgFormat(
    `
      CREATE SCHEMA %I ;

      CREATE TABLE %I.%I (
        foo   INTEGER PRIMARY KEY,
        bar   TEXT,
        baz   DATE
      ) ;
    `,
    table_schema,
    table_schema,
    table_name
  );

  await dama_db.query(ddl, PG_ENV);

  const dama_src_name = uuid().slice(0, 10);

  const { source_id } = await dama_meta.createNewDamaSource(
    {
      name: dama_src_name,
    },
    PG_ENV
  );

  await dama_meta.createNewDamaView(
    {
      source_id,
      table_schema,
      table_name,
    },
    PG_ENV
  );

  const table_description = await dama_meta.describeTable(
    table_schema,
    table_name,
    PG_ENV
  );

  expect(table_description.foo.column_type).toBe("integer");
  expect(table_description.foo.column_number).toBe(1);

  expect(table_description.bar.column_type).toBe("text");
  expect(table_description.bar.column_number).toBe(2);

  expect(table_description.baz.column_type).toBe("date");
  expect(table_description.baz.column_number).toBe(3);
});

test("does _NOT_ describe non-data_manager tables", async () => {
  const table_name = getRandomTableName();

  const ddl = pgFormat(
    `
      CREATE TABLE public.%I (
        foo   INTEGER PRIMARY KEY,
        bar   TEXT,
        baz   DATE
      ) ;
    `,
    table_name
  );

  await dama_db.query(ddl, PG_ENV);

  expect(async () => {
    await dama_meta.describeTable("public", table_name, PG_ENV);
  }).rejects.toThrow(`No such table in data_manager: public.${table_name}`);
});
