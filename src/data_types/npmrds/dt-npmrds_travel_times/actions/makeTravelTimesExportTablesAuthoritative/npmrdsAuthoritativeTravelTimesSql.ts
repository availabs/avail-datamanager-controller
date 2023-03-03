import dedent from "dedent";
import pgFormat from "pg-format";
import _ from "lodash";

import { stateAbbr2FipsCode } from "../../../../../data_utils/constants/stateFipsCodes";

import { NpmrdsDatabaseSchemas, NpmrdsDataSources } from "../../../domain";

import {
  NodePgDbConnection,
  ParsedNpmrdsTravelTimesExportTableMetadata,
  EttViewsMetaSummary,
} from "./domain";

const schemaName = NpmrdsDatabaseSchemas.NpmrdsTravelTimes;

export const getNpmrdsStateYearMonthTableName = (
  state: string,
  year: number,
  month: number
) => {
  const mm = `0${month}`.slice(-2);

  return `npmrds_${state}_${year}${mm}`;
};

export async function createAuthoritativeRootTable(dbConn: NodePgDbConnection) {
  const sql = dedent(
    `
      CREATE TABLE IF NOT EXISTS public.npmrds_test (
        tmc                               VARCHAR(9),
        date                              DATE,
        epoch                             SMALLINT,
        travel_time_all_vehicles          REAL,
        travel_time_passenger_vehicles    REAL,
        travel_time_freight_trucks        REAL,
        data_density_all_vehicles         CHAR,
        data_density_passenger_vehicles   CHAR,
        data_density_freight_trucks       CHAR,
        state                             CHAR(2) NOT NULL
      )
        PARTITION BY LIST (state)
      ;
    `
  );

  const result = await dbConn.query(sql);

  return result;
}

export async function createAuthoritativePartitionsSchema(
  dbConn: NodePgDbConnection
) {
  const sql = dedent(
    pgFormat(
      `
        CREATE SCHEMA IF NOT EXISTS %I ;
      `,
      schemaName
    )
  );

  await dbConn.query(sql);

  return { schemaName };
}

export async function createAuthoritativeStateTable(
  dbConn: NodePgDbConnection,
  state: string
) {
  const tableName = `npmrds_${state}`;

  await createAuthoritativePartitionsSchema(dbConn);
  await createAuthoritativeRootTable(dbConn);

  const sql = dedent(
    pgFormat(
      `
        CREATE TABLE IF NOT EXISTS %I.%I
          PARTITION OF public.npmrds_test
          FOR VALUES IN (%L)
          PARTITION BY RANGE (date)
        ;
      `,
      schemaName,
      tableName,
      state
    )
  );

  await dbConn.query(sql);

  return { schemaName, tableName };
}

export async function createAuthoritativeStateYearTable(
  dbConn: NodePgDbConnection,
  state: string,
  year: number
) {
  const { tableName: parentTableName } = await createAuthoritativeStateTable(
    dbConn,
    state
  );

  const tableName = `npmrds_${state}_${year}`;
  const sql = dedent(
    pgFormat(
      `
        CREATE TABLE IF NOT EXISTS %I.%I
          PARTITION OF %I.%I
          FOR VALUES FROM (%L) TO (%L)
          PARTITION BY RANGE (date)
        ;
      `,
      schemaName,
      tableName,
      schemaName,
      parentTableName,
      `${year}-01-01`,
      `${year + 1}-01-01`
    )
  );

  await dbConn.query(sql);

  return { schemaName, tableName };
}

export async function createAuthoritativeStateYearMonthTable(
  dbConn: NodePgDbConnection,
  state: string,
  year: number,
  month: number
) {
  const { tableName: parentTableName } =
    await createAuthoritativeStateYearTable(dbConn, state, year);

  const tableName = getNpmrdsStateYearMonthTableName(state, year, month);

  const startDate = new Date(year, month - 1, 1)
    .toISOString()
    .replace(/T.*/, "");

  const endDate = new Date(year, month, 1).toISOString().replace(/T.*/, "");

  const sql = dedent(
    pgFormat(
      `
        CREATE TABLE IF NOT EXISTS %I.%I
          PARTITION OF %I.%I
          FOR VALUES FROM (%L) TO (%L)
          PARTITION BY RANGE (date)
        ;
      `,
      schemaName,
      tableName,
      schemaName,
      parentTableName,
      startDate,
      endDate
    )
  );

  await dbConn.query(sql);

  return { schemaName, tableName };
}

export async function attachPartitionTable(
  dbConn: NodePgDbConnection,
  authoritativeSchemaName: string,
  authoritativeTableName: string,
  ettTableSchema: string,
  ettTableName: string,
  dateRangeStart: string,
  dateRangeEnd: string
) {
  const attachPartitionSql = dedent(
    pgFormat(
      `
        ALTER TABLE %I.%I
          ATTACH PARTITION %I.%I
          FOR VALUES FROM (%L) TO (%L)
        ;
      `,
      authoritativeSchemaName,
      authoritativeTableName,
      ettTableSchema,
      ettTableName,
      dateRangeStart,
      dateRangeEnd
    )
  );

  await dbConn.query(attachPartitionSql);
}

export async function detachAttView(
  dbConn: NodePgDbConnection,
  attView: ParsedNpmrdsTravelTimesExportTableMetadata
) {
  const { state, year, month, tableSchema, tableName } = attView;

  const npmrdsStateYrMoTableName = getNpmrdsStateYearMonthTableName(
    state,
    year,
    month
  );

  const dettachPartitionSql = dedent(
    pgFormat(
      `
        ALTER TABLE %I.%I
          DETACH PARTITION %I.%I
        ;
      `,
      schemaName,
      npmrdsStateYrMoTableName,
      tableSchema,
      tableName
    )
  );

  await dbConn.query(dettachPartitionSql);
}

export async function updateNpmrdsAuthTravTimesViewMeta(
  dbConn: NodePgDbConnection,
  prevNpmrdsAuthTravTimesViewMeta: any,
  attViewsToDetach: ParsedNpmrdsTravelTimesExportTableMetadata[],
  attViewsMeta: EttViewsMetaSummary | null,
  ettViewsMeta: EttViewsMetaSummary,
  dateExtentsByState: Record<string, [string, string]>,
  etl_context_id: number
) {
  const {
    sortedByStateThenStartDate: ettViewIds,
    byMonthByYearByState: ettViewIdsByMonthByYearByState,
    dataDateRange: [ettStartDate, ettEndDate],
    lastUpdated: ettLastUpdated,
  } = ettViewsMeta;

  const {
    sortedByStateThenStartDate: attViewIds = [],
    byMonthByYearByState: attViewIdsByMonthByYearByState = {},
    dataDateRange: [attStartDate = ettStartDate, attEndDate = ettEndDate] = [],
    lastUpdated: attLastUpdated = ettLastUpdated,
  } = attViewsMeta || {};

  const data_start_date =
    attStartDate < ettStartDate ? attStartDate : ettStartDate;

  const data_end_date = attEndDate > ettEndDate ? attEndDate : ettEndDate;

  const lastUpdated =
    attLastUpdated > ettLastUpdated ? attLastUpdated : ettLastUpdated;

  const dataStartDateNumeric = data_start_date.replace(/[^0-9]/g, "");
  const dataEndDateNumeric = data_end_date.replace(/[^0-9]/g, "");

  const intervalVersion = `${dataStartDateNumeric}-${dataEndDateNumeric}`;

  const detachedAttViewIds = new Set(
    attViewsToDetach.map(({ damaViewId }) => damaViewId)
  );

  const viewDependencies = [
    ...attViewIds.filter((attViewId) => !detachedAttViewIds.has(attViewId)),
    ...ettViewIds,
  ].sort((a, b) => a - b);

  const stateFipsCSL = _.uniq([
    ...Object.keys(attViewIdsByMonthByYearByState),
    ...Object.keys(ettViewIdsByMonthByYearByState),
  ])
    .map((state) => stateAbbr2FipsCode[state])
    .sort()
    .join();

  const { view_id: prevDamaViewId = null, version: prevVersion = 0 } =
    prevNpmrdsAuthTravTimesViewMeta || {};

  const newVersion = +prevVersion + 1;

  const metadata = {
    dama: {
      versionLinkedList: {
        previous: prevDamaViewId,
        next: null,
      },
    },
    dateExtentsByState,
  };

  const insertSql = dedent(`
    INSERT INTO data_manager.views (
      table_schema,
      table_name,
      active_start_timestamp,

      source_id,                      -- $1 NpmrdsTravelTimes name

      interval_version,               -- $2
      geography_version,              -- $3
      version,                        -- $4  
      start_date,                     -- $5
      end_date,                       -- $6
      last_updated,                   -- $7
      view_dependencies,              -- $8
      metadata,                       -- $9
      etl_context_id                  -- $10
    ) VALUES (
      'public',
      'npmrds_test',
      NOW(),

      (
        SELECT
            source_id
          FROM data_manager.sources
          WHERE ( name = $1 )
      ),

      $2, $3, $4, $5, $6, $7, $8, $9, $10

    ) RETURNING *
  `);

  const values = [
    NpmrdsDataSources.NpmrdsTravelTimes,
    intervalVersion,
    stateFipsCSL,
    newVersion,
    data_start_date,
    data_end_date,
    lastUpdated,
    viewDependencies,
    metadata,
    etl_context_id,
  ];

  const {
    // rows: [{ view_id: newDamaViewId, active_start_timestamp }],
    rows: [newDamaViewMeta],
  } = await dbConn.query({ text: insertSql, values });

  const { view_id: newDamaViewId } = newDamaViewMeta;

  if (prevDamaViewId !== null) {
    const updateSql = dedent(`
      WITH cte_ts AS (
        SELECT
            active_start_timestamp AS active_end_timestamp
          FROM data_manager.views
          WHERE ( view_id = $1 )
      )
        UPDATE data_manager.views
          SET
            metadata = jsonb_set(metadata, ARRAY['dama', 'versionLinkedList', 'next'], $1::TEXT::JSONB),
            active_end_timestamp = ( SELECT active_end_timestamp FROM cte_ts )
          WHERE ( view_id = $2 )
    `);

    await dbConn.query({
      text: updateSql,
      values: [newDamaViewId, prevDamaViewId],
    });
  }

  return newDamaViewMeta;
}
