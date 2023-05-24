import dedent from "dedent";
import pgFormat from "pg-format";
import _ from "lodash";

import dama_db from "data_manager/dama_db";

import { stateAbbr2FipsCode } from "../../../../../data_utils/constants/stateFipsCodes";

import {
  NpmrdsDatabaseSchemas,
  NpmrdsDataSources,
  NpmrdsState,
} from "data_types/npmrds/domain";

import {
  ParsedNpmrdsTravelTimesExportTableMetadata,
  EttViewsMetaSummary,
} from "./domain";

import create_state_npmrds_travel_times_table from "../../ddl/create_state_npmrds_travel_times_table";

const npmrds_travel_times_schema = NpmrdsDatabaseSchemas.NpmrdsTravelTimes;

export const getNpmrdsStateYearMonthTableName = (
  state: string,
  year: number,
  month: number
) => {
  const mm = `0${month}`.slice(-2);

  return `npmrds_${state}_${year}${mm}`;
};

export async function createAuthoritativePartitionsSchema() {
  const sql = dedent(
    pgFormat(
      `
        CREATE SCHEMA IF NOT EXISTS %I ;
      `,
      npmrds_travel_times_schema
    )
  );

  await dama_db.query(sql);
}

export async function createAuthoritativeStateYearTable(
  state: NpmrdsState,
  year: number
) {
  const { table_schema: parent_table_schema, table_name: parent_table_name } =
    await create_state_npmrds_travel_times_table(state);

  await createAuthoritativePartitionsSchema();

  const table_name = `npmrds_${state}_${year}`;

  const sql = dedent(
    pgFormat(
      `
        CREATE TABLE IF NOT EXISTS %I.%I
          PARTITION OF %I.%I
          FOR VALUES FROM (%L) TO (%L)
          PARTITION BY RANGE (date)
        ;
      `,
      npmrds_travel_times_schema,
      table_name,
      parent_table_schema,
      parent_table_name,
      `${year}-01-01`,
      `${year + 1}-01-01`
    )
  );

  await dama_db.query(sql);

  return { table_schema: npmrds_travel_times_schema, table_name };
}

export async function createAuthoritativeStateYearMonthTable(
  state: NpmrdsState,
  year: number,
  month: number
) {
  const { table_name: parent_table_name } =
    await createAuthoritativeStateYearTable(state, year);

  const table_name = getNpmrdsStateYearMonthTableName(state, year, month);

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
      npmrds_travel_times_schema,
      table_name,
      npmrds_travel_times_schema,
      parent_table_name,
      startDate,
      endDate
    )
  );

  await dama_db.query(sql);

  return { table_schema: npmrds_travel_times_schema, table_name };
}

export async function attachPartitionTable(
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

  await dama_db.query(attachPartitionSql);
}

export async function detachAttView(
  attView: ParsedNpmrdsTravelTimesExportTableMetadata
) {
  const { state, year, month, table_schema, table_name } = attView;

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
      npmrds_travel_times_schema,
      npmrdsStateYrMoTableName,
      table_schema,
      table_name
    )
  );

  await dama_db.query(dettachPartitionSql);
}

export async function updateNpmrdsAuthTravTimesViewMeta(
  prevNpmrdsAuthTravTimesViewMeta: any,
  attViewsToDetach: ParsedNpmrdsTravelTimesExportTableMetadata[],
  attViewsMeta: EttViewsMetaSummary | null,
  ettViewsMeta: EttViewsMetaSummary,
  dateExtentsByState: Record<string, [string, string]>
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

  const start_date = attStartDate < ettStartDate ? attStartDate : ettStartDate;

  const end_date = attEndDate > ettEndDate ? attEndDate : ettEndDate;

  const lastUpdated =
    attLastUpdated > ettLastUpdated ? attLastUpdated : ettLastUpdated;

  const dataStartDateNumeric = start_date.replace(/[^0-9]/g, "");
  const dataEndDateNumeric = end_date.replace(/[^0-9]/g, "");

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
      metadata                        -- $9
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

      $2, $3, $4, $5, $6, $7, $8, $9

    ) RETURNING *
  `);

  const values = [
    NpmrdsDataSources.NpmrdsTravelTimes,
    intervalVersion,
    stateFipsCSL,
    newVersion,
    start_date,
    end_date,
    lastUpdated,
    viewDependencies,
    metadata,
  ];

  const {
    // rows: [{ view_id: newDamaViewId, active_start_timestamp }],
    rows: [newDamaViewMeta],
  } = await dama_db.query({ text: insertSql, values });

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

    await dama_db.query({
      text: updateSql,
      values: [newDamaViewId, prevDamaViewId],
    });
  }

  return newDamaViewMeta;
}
