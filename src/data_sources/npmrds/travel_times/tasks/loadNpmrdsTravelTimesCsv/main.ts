import { pipeline } from "stream";
import { promisify } from "util";
import { existsSync } from "fs";

import { Client as PostgresDB } from "pg";
import { from as copyFrom } from "pg-copy-streams";
import pgFormat from "pg-format";

import dedent from "dedent";
import _ from "lodash";
import memoize from "memoize-one";

import { format as csvFormat } from "fast-csv";

import Database, { Database as SQLiteDB } from "better-sqlite3";

import {
  PgEnv,
  getNodePgCredentials,
} from "../../../../../data_manager/dama_db/postgres/PostgreSQL";

const pipelineAsync = promisify(pipeline);

const schemaName = "npmrds_travel_times_partitions";

const columns = [
  "tmc",
  "date",
  "epoch",
  "travel_time_all_vehicles",
  "travel_time_passenger_vehicles",
  "travel_time_freight_trucks",
  "data_density_all_vehicles",
  "data_density_passenger_vehicles",
  "data_density_freight_trucks",
];

const getMetadataFromSqliteDb = memoize((sqliteDB: SQLiteDB) => {
  const metadata = sqliteDB
    .prepare(
      `
        SELECT
            name,
            LOWER(name) AS table_name, -- So PostgreSQL doesn't make the "T" capitalized.
            state,
            year,
            data_start_date,
            data_end_date,
            is_expanded,
            is_complete_month,
            download_timestamp
          FROM metadata
      `
    )
    .get();

  return metadata;
});

function createPostgesDbTable(sqliteDB: SQLiteDB, pgDB: PostgresDB) {
  const { table_name, state, data_start_date, data_end_date } =
    getMetadataFromSqliteDb(sqliteDB);

  const endDate = new Date(data_end_date);
  endDate.setDate(endDate.getDate() + 1);
  const endDateExclusive = endDate.toISOString().replace(/T.*/, "");

  // NOTE:  The CHECK constraints allow us to later attach the table to the partitioned npmrds table.
  // https://www.postgresql.org/docs/current/ddl-partitioning.html#DDL-PARTITIONING-DECLARATIVE-MAINTENANCE
  const sql = dedent(
    pgFormat(
      `
        CREATE SCHEMA IF NOT EXISTS %I ;

        CREATE TABLE %I.%I (
          tmc                               VARCHAR(9),
          date                              DATE,
          epoch                             SMALLINT,
          travel_time_all_vehicles          REAL,
          travel_time_passenger_vehicles    REAL,
          travel_time_freight_trucks        REAL,
          data_density_all_vehicles         CHAR,
          data_density_passenger_vehicles   CHAR,
          data_density_freight_trucks       CHAR,
          state                             CHAR(2),

          PRIMARY KEY ( tmc, date, epoch ),

          -- 
          CONSTRAINT %I CHECK ( state = %L ),
          CONSTRAINT %I CHECK(
            (date >= DATE %L )
            AND
            (date < %L )
          )
        );
      `,
      schemaName,
      schemaName,
      table_name,
      `${table_name}_state_chk`,
      state,
      `${table_name}_date_chk`,
      data_start_date,
      endDateExclusive
    )
  );

  pgDB.query(sql);

  return { table_schema: schemaName, table_name };
}

function createDataIterator(sqliteDB: SQLiteDB) {
  return sqliteDB
    .prepare(
      `
        SELECT ${columns}
          FROM npmrds_travel_times
      `
    )
    .iterate();
}

async function loadPostgresDbTable(sqliteDB: SQLiteDB, pgDB: PostgresDB) {
  const { table_name } = getMetadataFromSqliteDb(sqliteDB);

  const copyFromSql = pgFormat(
    `COPY %I.%I (${columns}) FROM STDIN WITH CSV`,
    schemaName,
    table_name
  );

  await pipelineAsync(
    createDataIterator(sqliteDB),
    csvFormat({ quote: true }),
    pgDB.query(copyFrom(copyFromSql))
  );
}

async function clusterPostgresTable(sqliteDB: SQLiteDB, pgDB: PostgresDB) {
  const { table_name } = getMetadataFromSqliteDb(sqliteDB);

  const sql = pgFormat(
    "CLUSTER %I.%I USING %I ;",
    schemaName,
    table_name,
    `${table_name}_pkey`
  );

  await pgDB.query(sql);
}

export default async function main({
  npmrds_export_sqlite_db_path,
  pgEnv,
}: {
  npmrds_export_sqlite_db_path: string;
  pgEnv: PgEnv;
}) {
  if (!existsSync(npmrds_export_sqlite_db_path)) {
    throw new Error("The npmrds_export_sqlite_db_path file does not exists.");
  }

  const sqliteDB = new Database(npmrds_export_sqlite_db_path, {
    readonly: true,
  });

  const nodePgCreds = getNodePgCredentials(pgEnv);
  const pgDB = new PostgresDB(nodePgCreds);
  await pgDB.connect();

  const metadata = getMetadataFromSqliteDb(sqliteDB);

  await pgDB.query("BEGIN ;");
  console.log("begin");

  const { table_schema, table_name } = createPostgesDbTable(sqliteDB, pgDB);
  console.log("created table");

  console.log("loading table");
  await loadPostgresDbTable(sqliteDB, pgDB);
  console.log("loaded table");
  await clusterPostgresTable(sqliteDB, pgDB);
  console.log("clustered table");

  await pgDB.query("COMMIT ;");
  console.log("commited");

  await pgDB.end();
  sqliteDB.close();

  return { metadata, table_schema, table_name };
}
