#!/usr/bin/env node

import yargs from "yargs";
import pgFormat from "pg-format";

import {
  makeRawTranscomEventExpandedIteratorFromApiScrapeFile,
  transformRawTranscomEventExpandedIteratorToProtoTranscomEventExpandedIterator,
  loadProtoTranscomEventsExpandedIntoDatabase,
} from "..";

import {
  getConnectedPgClient,
  cliArgsSpec as pgEnvCliArgsSpec,
} from "../../../utils/PostgreSQL";

async function loadTranscomEventsExpandedFile({
  pg_env,
  apiScrapeFilePath,
  tableSchema,
  tableName,
}) {
  const db = await getConnectedPgClient(pg_env);

  try {
    await db.query("BEGIN;");

    const sql = pgFormat(
      `
        CREATE TABLE IF NOT EXISTS %I.%I (
          LIKE _transcom_admin.transcom_events_expanded
            INCLUDING DEFAULTS      -- necessary for _created_timestamp & _modified_timestamp
            EXCLUDING CONSTRAINTS   -- because scrapes may violate PrimaryKey CONSTRAINT
        ) ;
      `,
      tableSchema,
      tableName
    );

    await db.query(sql);

    const rawIter =
      makeRawTranscomEventExpandedIteratorFromApiScrapeFile(apiScrapeFilePath);

    const protoIter =
      transformRawTranscomEventExpandedIteratorToProtoTranscomEventExpandedIterator(
        rawIter
      );

    await loadProtoTranscomEventsExpandedIntoDatabase(
      protoIter,
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

if (!module.parent) {
  const { argv } = yargs
    .strict()
    .parserConfiguration({
      "camel-case-expansion": false,
      "flatten-duplicate-arrays": false,
    })
    .wrap(yargs.terminalWidth() / 1.618)
    // @ts-ignore
    .option({
      ...pgEnvCliArgsSpec,
      apiScrapeFilePath: {
        type: "string",
        demand: true,
        description:
          "Path to the directory containing the scraped raw-transcom-events.",
      },
      tableSchema: {
        type: "string",
        demand: true,
        description: "Schema of the table to load.",
      },
      tableName: {
        type: "string",
        demand: true,
        description: "Name of the table to load (and possibly create).",
      },
    });

  // @ts-ignore
  loadTranscomEventsExpandedFile(argv);
} else {
  module.exports = loadTranscomEventsExpandedFile;
}
