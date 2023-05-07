import { existsSync } from "fs";
import { pipeline } from "stream";
import { promisify } from "util";

import { from as copyFrom } from "pg-copy-streams";
import dedent from "dedent";
import pgFormat from "pg-format";

import memoize from "memoize-one";

import { format as csvFormat } from "fast-csv";

import Database, { Database as SQLiteDB } from "better-sqlite3";

import dama_db from "data_manager/dama_db";
import logger from "data_manager/logger";

import { NpmrdsDatabaseSchemas } from "data_types/npmrds/domain";

export type DoneData = {
  metadata: {
    state: string;
    year: number;
    download_timestamp: string;
  };
  table_schema: string;
  table_name: string;
};

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

const getMetadataFromSqliteDb = memoize((sqlite_db: SQLiteDB) => {
  const { state, year, download_timestamp } = sqlite_db
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

async function createPostgesDbTable(sqlite_db: SQLiteDB) {
  const { state, table_schema, table_name } =
    getMetadataFromSqliteDb(sqlite_db);

  if (process.env.NODE_ENV?.toLowerCase() === "development") {
    await dama_db.query(
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

  await dama_db.query(sql);

  return { table_schema, table_name };
}

function* createDataIterator(sqlite_db: SQLiteDB) {
  const { state, download_timestamp } = getMetadataFromSqliteDb(sqlite_db);

  const iter = sqlite_db
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

async function loadPostgresDbTable(sqlite_db: SQLiteDB) {
  const { table_schema, table_name } = getMetadataFromSqliteDb(sqlite_db);

  const pg_db = await dama_db.getDbConnection();

  const copy_from_sql = pgFormat(
    `COPY %I.%I (${columns}) FROM STDIN WITH CSV`,
    table_schema,
    table_name
  );

  await pipelineAsync(
    createDataIterator(sqlite_db),
    csvFormat({ quote: true }),
    pg_db.query(copyFrom(copy_from_sql))
  );
}

async function clusterPostgresTable(sqlite_db: SQLiteDB) {
  const { table_schema, table_name } = getMetadataFromSqliteDb(sqlite_db);

  const sql = pgFormat(
    "CLUSTER %I.%I USING %I ;",
    table_schema,
    table_name,
    `${table_name}_pkey`
  );

  await dama_db.query(sql);
}

async function analyzePostgresTable(sqlite_db: SQLiteDB) {
  const { table_schema, table_name } = getMetadataFromSqliteDb(sqlite_db);

  const sql = pgFormat("ANALYZE %I.%I ;", table_schema, table_name);

  await dama_db.query(sql);
}

export default async function main(
  npmrds_travel_times_sqlite_db: string
): Promise<DoneData> {
  if (!existsSync(npmrds_travel_times_sqlite_db)) {
    throw new Error(
      `NpmrdsTravelTimesExportSqlite file ${npmrds_travel_times_sqlite_db} does not exists.`
    );
  }

  const sqlite_db = new Database(npmrds_travel_times_sqlite_db, {
    readonly: true,
  });

  const metadata = getMetadataFromSqliteDb(sqlite_db);

  const { table_schema, table_name } = await dama_db.runInTransactionContext(
    async () => {
      logger.debug("creating tmc_identification table");
      const create_table_result = await createPostgesDbTable(sqlite_db);
      logger.debug("created tmc_identification table");

      logger.debug("loading tmc_identification table");
      await loadPostgresDbTable(sqlite_db);
      logger.debug("loaded table");

      await clusterPostgresTable(sqlite_db);
      logger.debug("clustered table");

      sqlite_db.close();

      return create_table_result;
    }
  );

  await analyzePostgresTable(sqlite_db);
  logger.debug("analyzed table");

  return { metadata, table_schema, table_name };
}
