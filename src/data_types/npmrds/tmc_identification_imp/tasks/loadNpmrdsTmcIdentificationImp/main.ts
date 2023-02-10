import { existsSync } from "fs";
import { pipeline } from "stream";
import { promisify } from "util";

import { Client as PostgresDB } from "pg";
import { from as copyFrom } from "pg-copy-streams";
import dedent from "dedent";
import pgFormat from "pg-format";

import memoize from "memoize-one";

import { format as csvFormat } from "fast-csv";

import Database, { Database as SQLiteDB } from "better-sqlite3";

import { NpmrdsDatabaseSchemas } from "../../../domain";

import {
  PgEnv,
  getNodePgCredentials,
} from "../../../../../data_manager/dama_db/postgres/PostgreSQL";

const pipelineAsync = promisify(pipeline);

const columns = [
  "tmc",
  "type",
  "road",
  "road_order",
  "intersection",
  "tmclinear",
  "country",
  "state",
  "county",
  "zip",
  "direction",
  "start_latitude",
  "start_longitude",
  "end_latitude",
  "end_longitude",
  "miles",
  "frc",
  "border_set",
  "isprimary",
  "f_system",
  "urban_code",
  "faciltype",
  "structype",
  "thrulanes",
  "route_numb",
  "route_sign",
  "route_qual",
  "altrtename",
  "aadt",
  "aadt_singl",
  "aadt_combi",
  "nhs",
  "nhs_pct",
  "strhnt_typ",
  "strhnt_pct",
  "truck",
  "timezone_name",
  "active_start_date",
  "active_end_date",
  "download_timestamp",
];

const getMetadataFromSqliteDb = memoize((sqliteDB: SQLiteDB) => {
  const { state, year, download_timestamp } = sqliteDB
    .prepare(
      `
        SELECT
            state,
            year,
            download_timestamp
          FROM metadata
      `
    )
    .get();

  const ts = download_timestamp.replace(/[^0-9T]/gi, "").toLowerCase();

  const table_schema = NpmrdsDatabaseSchemas.NpmrdsTmcIdentificationImp;
  const table_name = `tmc_identification_${state}_${year}_v${ts}`;

  return {
    state,
    year,
    download_timestamp,
    table_schema,
    table_name,
  };
});

async function createPostgesDbTable(sqliteDB: SQLiteDB, pgDB: PostgresDB) {
  const { state, table_schema, table_name } = getMetadataFromSqliteDb(sqliteDB);

  if (process.env.NODE_ENV?.toLowerCase() === "development") {
    await pgDB.query(
      pgFormat("DROP TABLE IF EXISTS %I.%I;", table_schema, table_name)
    );
  }

  const sql = dedent(
    pgFormat(
      `
        CREATE SCHEMA IF NOT EXISTS %I ;

        CREATE TABLE IF NOT EXISTS %I.%I (

          tmc                      CHARACTER VARYING PRIMARY KEY,
          type                     CHARACTER VARYING,
          road                     CHARACTER VARYING,
          road_order               REAL,
          intersection             CHARACTER VARYING,
          tmclinear                INTEGER,
          country                  CHARACTER VARYING,
          state                    CHARACTER VARYING,
          county                   CHARACTER VARYING,
          zip                      CHARACTER VARYING,
          direction                CHARACTER VARYING,
          start_latitude           DOUBLE PRECISION,
          start_longitude          DOUBLE PRECISION,
          end_latitude             DOUBLE PRECISION,
          end_longitude            DOUBLE PRECISION,
          miles                    DOUBLE PRECISION,
          frc                      SMALLINT,
          border_set               CHARACTER VARYING,
          isprimary                SMALLINT,
          f_system                 SMALLINT,
          urban_code               INTEGER,
          faciltype                SMALLINT,
          structype                SMALLINT,
          thrulanes                SMALLINT,
          route_numb               INTEGER,
          route_sign               SMALLINT,
          route_qual               SMALLINT,
          altrtename               CHARACTER VARYING,
          aadt                     INTEGER,
          aadt_singl               INTEGER,
          aadt_combi               INTEGER,
          nhs                      SMALLINT,
          nhs_pct                  SMALLINT,
          strhnt_typ               SMALLINT,
          strhnt_pct               SMALLINT,
          truck                    SMALLINT,
          timezone_name            CHARACTER VARYING,
          active_start_date        DATE,
          active_end_date					 DATE,
          download_timestamp 			 TIMESTAMP,

          -- The following CHECK CONSTRAINT allows the table to later be ATTACHed
          --   to the NpmrdsTmcIdentificationAdv PARTITIONed TABLE hierarchy.

          CONSTRAINT npmrds_state_chk CHECK ( state = %L )

        ) ;
      `,
      table_schema,
      table_schema,
      table_name,
      state.toUpperCase()
    )
  );

  await pgDB.query(sql);

  return { table_schema, table_name };
}

function* createDataIterator(sqliteDB: SQLiteDB) {
  const { state, download_timestamp } = getMetadataFromSqliteDb(sqliteDB);

  const iter = sqliteDB
    .prepare(
      `
        SELECT ${columns}
          FROM tmc_identification
            CROSS JOIN (
              SELECT
                  '${download_timestamp}' AS download_timestamp
            )
          WHERE ( UPPER(state) = ? )
      `
    )
    .iterate([state.toUpperCase()]);

  for (const row of iter) {
    columns.forEach((c) => {
      if (/^null$/i.test(row[c])) {
        row[c] = null;
      }
    });

    yield row;
  }
}

async function loadPostgresDbTable(sqliteDB: SQLiteDB, pgDB: PostgresDB) {
  const { table_schema, table_name } = getMetadataFromSqliteDb(sqliteDB);

  const copyFromSql = pgFormat(
    `COPY %I.%I (${columns}) FROM STDIN WITH CSV`,
    table_schema,
    table_name
  );

  await pipelineAsync(
    createDataIterator(sqliteDB),
    csvFormat({ quote: true }),
    pgDB.query(copyFrom(copyFromSql))
  );
}

async function clusterPostgresTable(sqliteDB: SQLiteDB, pgDB: PostgresDB) {
  const { table_schema, table_name } = getMetadataFromSqliteDb(sqliteDB);

  const sql = pgFormat(
    "CLUSTER %I.%I USING %I ;",
    table_schema,
    table_name,
    `${table_name}_pkey`
  );

  await pgDB.query(sql);
}

async function analyzePostgresTable(sqliteDB: SQLiteDB, pgDB: PostgresDB) {
  const { table_schema, table_name } = getMetadataFromSqliteDb(sqliteDB);

  const sql = pgFormat("ANALYZE %I.%I ;", table_schema, table_name);

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

  const { table_schema, table_name } = await createPostgesDbTable(
    sqliteDB,
    pgDB
  );
  console.log("created table");

  console.log("loading table");
  await loadPostgresDbTable(sqliteDB, pgDB);
  console.log("loaded table");
  await clusterPostgresTable(sqliteDB, pgDB);
  console.log("clustered table");

  await pgDB.query("COMMIT ;");
  console.log("commited");

  await analyzePostgresTable(sqliteDB, pgDB);
  console.log("analyzed table");

  await pgDB.end();
  sqliteDB.close();

  return { metadata, table_schema, table_name };
}
