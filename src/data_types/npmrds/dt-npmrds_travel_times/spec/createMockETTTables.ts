/*
  ./node_modules/.bin/ts-node src/data_sources/npmrds/dt-npmrds_travel_times/spec/createMockETTTables.ts
*/
import dedent from "dedent";
import pgFormat from "pg-format";
import _ from "lodash";
import memoize from "memoize-one";
import { v4 as uuidv4 } from "uuid";
import { DateTime } from "luxon";

import {
  NodePgClient,
  getConnectedNodePgClient,
} from "../../../../data_manager/dama_db/postgres/PostgreSQL";

import { NpmrdsDataSources } from "../../domain";

import { stateAbbr2FipsCode } from "../../../../data_utils/constants/stateFipsCodes";

const NUM_TMCS = 1;
const STATES = ["ct", "nj", "ny", "pa"];

const DATA_START_DATE = "2021-01-01";
const DATA_END_DATE = "2022-12-31";

const EPOCHS = _.range(0, 288);

const PG_ENV = "dama_dev_1";

const createName = (
  state: string,
  startDate: string,
  endDate: string,
  ts: string
) => `npmrdsx_${state}_from_${startDate}_to_${endDate}_v${ts}`;

const timestamp = new Date(2023, 0, 1);

function generateMockEttMetadata(interval: "week" | "month") {
  const ettMeta: any[] = [];

  const endDate = DateTime.fromISO(DATA_END_DATE)
    .plus({ days: 1 })
    .startOf("day");

  for (const state of STATES) {
    let dt = DateTime.fromISO(DATA_START_DATE);

    while (dt.startOf("day") < endDate.startOf("day")) {
      const { year } = dt;

      const data_start_date = dt.toISODate();
      const startDateNumeric = dt.toFormat("yyyyMMdd");

      const endOfInterval = dt.endOf(interval);
      const endOfMonth = dt.endOf("month");

      dt =
        endOfInterval.startOf("day") < endOfMonth.startOf("day")
          ? endOfInterval
          : endOfMonth;

      const data_end_date = dt.toISODate();
      const endDateNumeric = dt.toFormat("yyyyMMdd");

      const download_timestamp = timestamp.toISOString().replace(/\..*/, "");
      const ts = timestamp.toISOString().replace(/[^0-9T]/g, "");

      const name = createName(state, startDateNumeric, endDateNumeric, ts);

      console.log(name);

      dt = dt.plus({ days: 1 });
      const endDateExclusive = dt.toISODate();

      const table_name = name.toLowerCase();

      ettMeta.push({
        name,
        year,
        state,
        table_schema: "npmrds_travel_times_imports",
        table_name,
        is_expanded: true,
        data_start_date,
        data_end_date,
        is_complete_month: interval === "month",
        download_timestamp,
        endDateExclusive,
      });

      timestamp.setHours(timestamp.getHours() + 1);
    }
  }

  return ettMeta;
}

const createTableSqlTemplate = dedent(
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
        state                             CHAR(2) NOT NULL DEFAULT %L,

        PRIMARY KEY ( tmc, date, epoch ),

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

    ALTER INDEX %I.%I
      SET (fillfactor=100)
    ;
  `
);

async function createMockEttTable(db: NodePgClient, meta: any) {
  const { table_schema, table_name, state, data_start_date, endDateExclusive } =
    meta;

  console.log(table_name);

  const sql = pgFormat(
    createTableSqlTemplate,
    table_schema,
    table_schema,
    table_name,
    state,
    state,
    data_start_date,
    endDateExclusive,
    table_schema,
    `${table_name}_pkey`
  );

  await db.query(sql);
}

const insertMockEttDataSqlTemplate = dedent(
  `
    INSERT INTO %I.%I (
      tmc,
      date,
      epoch,
      travel_time_all_vehicles,
      travel_time_passenger_vehicles,
      travel_time_freight_trucks
    ) VALUES ( $1, $2, $3, $4, $4, $4 ) ;
  `
);

async function loadMockEttData(db: NodePgClient, meta: any) {
  const { table_schema, table_name, data_start_date, data_end_date } = meta;

  console.time(`LOAD ${table_name}`);

  const tmcs: Set<string> = new Set();

  while (tmcs.size < NUM_TMCS) {
    tmcs.add(
      uuidv4()
        .replace(/[^0-9a-z]/gi, "")
        .slice(0, 9)
    );

    await new Promise((resolve) => process.nextTick(resolve));
  }

  const text = pgFormat(insertMockEttDataSqlTemplate, table_schema, table_name);

  const [yyyy, mm, startDate] = data_start_date.split(/-/);
  const [, , endDate] = data_end_date.split(/-/);

  const sDate = +startDate;
  const eDate = +endDate;

  let count = 0;
  for (const tmc of tmcs) {
    for (let curDate = sDate; curDate <= eDate; ++curDate) {
      const dd = `0${curDate}`.slice(-2);

      for (const epoch of EPOCHS) {
        const values = [tmc, `${yyyy}-${mm}-${dd}`, epoch, Math.random() * 100];

        await db.query({
          text,
          values,
        });

        ++count;
      }
    }
  }

  console.timeEnd(`LOAD ${table_name}`);

  return count;
}

const getEttDamaSrcId = memoize(async (db: NodePgClient) => {
  const text = "SELECT source_id FROM data_manager.sources WHERE name = $1 ;";
  const {
    rows: [{ source_id }],
  } = await db.query({
    text,
    values: [NpmrdsDataSources.NpmrdsTravelTimesImp],
  });

  return source_id;
});

async function insertMockEttDamaView(db: NodePgClient, meta: any) {
  const {
    name,
    state,
    table_schema,
    table_name,
    data_start_date,
    data_end_date,
    download_timestamp,
  } = meta;

  const ettDamaSrcId = await getEttDamaSrcId(db);

  const metadata = _.pick(meta, [
    "name",
    "year",
    "state",
    "table_name",
    "is_expanded",
    "data_start_date",
    "data_end_date",
    "is_complete_month",
    "download_timestamp",
  ]);

  const numericStartDate = data_start_date.replace(/[^0-9]g/, "");
  const numericEndDate = data_end_date.replace(/[^0-9]g/, "");

  const intervalVersion = `${numericStartDate}-${numericEndDate}`;

  const stateFips = stateAbbr2FipsCode[state];

  const text = dedent(`
    INSERT INTO data_manager.views (
      source_id,                  -- $1
      interval_version,           -- $2
      geography_version,          -- $3
      version,                    -- $4
      table_schema,               -- $5
      table_name,                 -- $6
      start_date,                 -- $7
      end_date,                   -- $8
      last_updated,               -- $9
      metadata                    -- $10
    ) VALUES (
      $1,
      $2,
      $3,
      $4,
      $5,
      $6,
      $7,
      $8,
      $9,
      $10
    ) RETURNING view_id ;
  `);

  const values = [
    ettDamaSrcId,
    intervalVersion,
    stateFips,
    name,
    table_schema,
    table_name,
    data_start_date,
    data_end_date,
    download_timestamp,
    metadata,
  ];

  const {
    rows: [{ view_id }],
  } = await db.query({
    text,
    values,
  });

  return view_id;
}

// curl 'localhost:3369/dama-admin/dama_dev_1/data-sources/npmrds/dt-npmrds_travel_times/makeTravelTimesExportTableAuthoritative?damaViewId=1739'

async function main() {
  const db = await getConnectedNodePgClient(PG_ENV);

  await db.query("BEGIN ;");
  await db.query("DELETE FROM data_manager.views WHERE view_id > 6 ;");
  await db.query("DROP SCHEMA IF EXISTS npmrds_travel_times_imports CASCADE ;");
  await db.query("DROP SCHEMA IF EXISTS npmrds_travel_times CASCADE ;");

  const ettMeta = {
    weekly: generateMockEttMetadata("week"),
    monthly: generateMockEttMetadata("month"),
  };

  const allMeta = [...ettMeta.weekly, ...ettMeta.monthly];

  const viewIds: any[] = [];

  for (const meta of allMeta) {
    await createMockEttTable(db, meta);
    await loadMockEttData(db, meta);
    const view_id = await insertMockEttDamaView(db, meta);

    viewIds.push(view_id);
  }

  await db.query("COMMIT ;");

  /*
  for (const viewId of viewIds) {
    const url = new URL(makeAuthoritativeUrl);
    url.searchParams.append("damaViewId", viewId);

    await fetch(url);
  }
  */

  await db.end();
}

main();
