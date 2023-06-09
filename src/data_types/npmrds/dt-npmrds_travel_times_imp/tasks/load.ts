import { pipeline } from "stream";
import { promisify } from "util";
import { existsSync } from "fs";

import { from as copyFrom } from "pg-copy-streams";
import pgFormat from "pg-format";

import dedent from "dedent";
import _ from "lodash";
import memoize from "memoize-one";

import { format as csvFormat } from "fast-csv";

import Database, { Database as SQLiteDB } from "better-sqlite3";

import dama_db from "data_manager/dama_db";
import logger from "data_manager/logger";

import { NpmrdsDatabaseSchemas } from "data_types/npmrds/domain";

import create_state_npmrds_travel_times_table from "data_types/npmrds/dt-npmrds_travel_times/ddl/create_state_npmrds_travel_times_table";

export type DoneData = {
  metadata: {
    name: string;
    state: string;
    year: string;
    start_date: string;
    end_date: string;
    is_complete_month: boolean;
    is_complete_week: boolean;
  };
  table_schema: string;
  table_name: string;
};

const pipelineAsync = promisify(pipeline);

const npmrds_travel_times_imports_schema_name =
  NpmrdsDatabaseSchemas.NpmrdsTravelTimesImports;

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

const getMetadataFromSqliteDb = memoize((sqlite_db: SQLiteDB) => {
  const metadata = sqlite_db
    .prepare(
      `
        SELECT
            name,
            name as table_name,
            state,
            year,
            start_date,
            end_date,
            is_expanded,
            is_complete_month,
            is_complete_week,
            download_timestamp
          FROM metadata
      `
    )
    .get();

  metadata.is_complete_month = !!metadata.is_complete_month;
  metadata.is_complete_week = !!metadata.is_complete_week;

  return metadata;
});

async function createPostgesDbTable(sqlite_db: SQLiteDB) {
  const { table_name, state, start_date, end_date } =
    getMetadataFromSqliteDb(sqlite_db);

  const endDate = new Date(end_date);
  endDate.setDate(endDate.getDate() + 1);
  const end_date_exclusive = endDate.toISOString().replace(/T.*/, "");

  if (process.env.NODE_ENV?.toLowerCase() === "development") {
    await dama_db.query(
      pgFormat(
        "DROP TABLE IF EXISTS %I.%I;",
        npmrds_travel_times_imports_schema_name,
        table_name
      )
    );
  }

  logger.debug(
    `dt-npmrds_travel_times_imp load CREATE TABLE params: ${JSON.stringify(
      {
        npmrds_travel_times_imports_schema_name,
        table_name,
        state,
        start_date,
        end_date_exclusive,
        index: `${table_name}_pkey`,
      },
      null,
      4
    )}`
  );

  const { table_schema: parent_table_schema, table_name: parent_table_name } =
    await create_state_npmrds_travel_times_table(state);

  // NOTE:  The CHECK constraints allow us to later attach the table to the partitioned npmrds table.
  // https://www.postgresql.org/docs/current/ddl-partitioning.html#DDL-PARTITIONING-DECLARATIVE-MAINTENANCE
  const sql = dedent(
    pgFormat(
      `
        CREATE SCHEMA IF NOT EXISTS %I ;

        CREATE TABLE %I.%I (
          LIKE %I.%I,

          PRIMARY KEY (tmc, date, epoch),

          -- The following CHECK CONSTRAINTs allow the table to later be ATTACHed
          --   to the NpmrdsTravelTimes PARTITIONed TABLE hierarchy.

          CONSTRAINT npmrds_state_chk CHECK ( state = %L ),

          CONSTRAINT npmrds_date_chk CHECK(
            (date >= DATE %L )
            AND
            (date < %L )
          )
        ) WITH ( fillfactor=100, autovacuum_enabled=false )
        ;

        ALTER TABLE %I.%I
          ALTER COLUMN state SET DEFAULT %L
        ;

        ALTER INDEX %I.%I
          SET (fillfactor=100)
        ;

        CLUSTER %I.%I USING %I ;
      `,
      npmrds_travel_times_imports_schema_name,

      npmrds_travel_times_imports_schema_name,
      table_name,
      parent_table_schema,
      parent_table_name,
      state,
      start_date,
      end_date_exclusive,

      npmrds_travel_times_imports_schema_name,
      table_name,
      state,

      npmrds_travel_times_imports_schema_name,
      `${table_name}_pkey`,

      npmrds_travel_times_imports_schema_name,
      table_name,
      `${table_name}_pkey`
    )
  );

  await dama_db.query(sql);

  return { table_schema: npmrds_travel_times_imports_schema_name, table_name };
}

function createDataIterator(sqlite_db: SQLiteDB) {
  return sqlite_db
    .prepare(
      `
        SELECT ${columns}
          FROM npmrds_travel_times
      `
    )
    .iterate();
}

async function loadPostgresDbTable(sqlite_db: SQLiteDB) {
  const pg_db_conn = await dama_db.getDbConnection();

  try {
    const { table_name } = getMetadataFromSqliteDb(sqlite_db);

    const clear_table = pgFormat(
      "DELETE FROM %I.%I",
      npmrds_travel_times_imports_schema_name,
      table_name
    );

    await dama_db.query(clear_table);

    const copyFromSql = pgFormat(
      `COPY %I.%I (${columns}) FROM STDIN WITH CSV`,
      npmrds_travel_times_imports_schema_name,
      table_name
    );

    await pipelineAsync(
      createDataIterator(sqlite_db),
      csvFormat({ quote: true }),
      pg_db_conn.query(copyFrom(copyFromSql))
    );
  } finally {
    try {
      pg_db_conn.release();
    } catch (err) {
      //
    }
  }
}

async function clusterPostgresTable(sqlite_db: SQLiteDB) {
  const { table_name } = getMetadataFromSqliteDb(sqlite_db);

  const sql = pgFormat(
    "CLUSTER %I.%I ;",
    npmrds_travel_times_imports_schema_name,
    table_name
  );

  await dama_db.query(sql);
}

async function analyzePostgresTable(table_name: string) {
  const sql = pgFormat(
    "ANALYZE %I.%I ;",
    npmrds_travel_times_imports_schema_name,
    table_name
  );

  await dama_db.query(sql);
}

export default async function main(
  npmrds_travel_times_sqlite_db: string
): Promise<DoneData> {
  if (!existsSync(npmrds_travel_times_sqlite_db)) {
    throw new Error("The npmrds_travel_times_sqlite_db file does not exists.");
  }

  const sqlite_db = new Database(npmrds_travel_times_sqlite_db, {
    readonly: true,
  });

  const metadata = getMetadataFromSqliteDb(sqlite_db);

  const { table_schema, table_name } = await dama_db.runInTransactionContext(
    async () => {
      logger.debug("creating npmrds_travel_times table");
      const create_table_result = await createPostgesDbTable(sqlite_db);
      logger.debug("created npmrds_travel_times table");

      logger.debug("loading npmrds_travel_times table");
      await loadPostgresDbTable(sqlite_db);
      logger.debug("loaded npmrds_travel_times table");

      await clusterPostgresTable(sqlite_db);
      logger.debug("clustered npmrds_travel_times table");

      return create_table_result;
    }
  );

  await analyzePostgresTable(table_name);
  logger.debug("analyzed npmrds_travel_times table");

  sqlite_db.close();

  return { metadata, table_schema, table_name };
}
