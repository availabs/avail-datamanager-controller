/*
    NpmrdsTmcDateRanges DamaViews have a single NpmrdsTravelTimes DamaViewID for its view_dependencies.

    After makeTravelTimesExportTablesAuthoritative, the NpmrdsTmcDateRanges will be outdated.

    We need to update the tmc_date_ranges table to include the newly authoritative NpmrdsTravelTimesImports.

    We do this by taking the set difference between the
      current authoritative NpmrdsTravelTimesImports and the
      authoritative NpmrdsTravelTimesImports when the NpmrdsTmcDateRanges were last updated.
*/

import dedent from "dedent";
import pgFormat from "pg-format";
import _ from "lodash";

import dama_db from "data_manager/dama_db";
import dama_meta from "data_manager/meta";
import logger from "data_manager/logger";

import { NpmrdsDataSources, NpmrdsState } from "../domain";
import { initializeDamaSources } from "../utils/dama_sources";

import create_state_tmc_date_ranges from "./ddl/create_state_tmc_date_ranges";

type NpmrdsTravelTimesImportInfo = {
  table_schema: string;
  table_name: string;
  state: NpmrdsState;
};

async function getNpmrdsTmcDateRangesSourceId() {
  const sql = dedent(`
    SELECT
        source_id
      FROM data_manager.sources
      WHERE ( name = $1 )
    ;
  `);

  const {
    rows: [{ source_id }],
  } = await dama_db.query({
    text: sql,
    values: [NpmrdsDataSources.NpmrdsTmcDateRanges],
  });

  return source_id;
}

// Get the NpmrdsTravelTimes view_id that the currently active NpmrdsTmcDateRanges depends on.
async function getActiveTmcDateRangesInfo(): Promise<{
  tmc_date_ranges_dama_source_id: number;
  old_tmc_date_ranges_dama_view_id: number;
  old_travel_times_dama_view_id: number;
} | null> {
  const sql = dedent(`
    SELECT
        b.view_id,
        b.view_dependencies
      FROM data_manager.sources AS a
        INNER JOIN data_manager.views AS b
          USING ( source_id )
      WHERE (
        ( a.name = $1 )
        AND
        ( b.active_start_timestamp IS NOT NULL )
        AND
        ( b.active_end_timestamp IS NULL )
      )
  `);

  const { rows } = await dama_db.query({
    text: sql,
    values: [NpmrdsDataSources.NpmrdsTmcDateRanges],
  });

  if (rows.length === 0) {
    return null;
  }

  if (rows.length > 1) {
    throw new Error(
      "INVARIANT BROKEN: More than 1 authoritative NpmrdsTmcDateRanges DamaView."
    );
  }

  const [
    {
      source_id: tmc_date_ranges_dama_source_id,
      view_id: old_tmc_date_ranges_dama_view_id,
      view_dependencies: [old_travel_times_dama_view_id],
    },
  ] = rows;

  return {
    tmc_date_ranges_dama_source_id,
    old_tmc_date_ranges_dama_view_id,
    old_travel_times_dama_view_id,
  };
}

// Get the currently active NpmrdsTravelTimes view_id and geography_version
async function getActiveTravelTimesViewInfo(): Promise<{
  view_id: number;
  geography_version: string;
  start_date: Date;
  end_date: Date;
} | null> {
  const sql = dedent(`
    SELECT
        b.view_id,
        b.geography_version,
        b.start_date,
        b.end_date
      FROM data_manager.sources AS a
        INNER JOIN data_manager.views AS b
          USING ( source_id )
      WHERE (
        ( a.name = $1 )
        AND
        ( b.active_start_timestamp IS NOT NULL )
        AND
        ( b.active_end_timestamp IS NULL )
      )
  `);

  const { rows } = await dama_db.query({
    text: sql,
    values: [NpmrdsDataSources.NpmrdsTravelTimes],
  });

  if (rows.length > 1) {
    throw new Error(
      "INVARIANT BROKEN: More than 1 authoritative NpmrdsTravelTimes DamaView."
    );
  }

  return rows.length ? rows[0] : null;
}

async function getNpmrdsTravelTimesImportInfo(
  travel_times_dama_view_id: number | null = null
): Promise<NpmrdsTravelTimesImportInfo[]> {
  console.log("travel_times_dama_view_id:", travel_times_dama_view_id);
  if (travel_times_dama_view_id === null) {
    return [];
  }

  const sql = dedent(`
    SELECT DISTINCT
        b.table_schema,
        b.table_name,
        c.state
      FROM data_manager.views AS a
        INNER JOIN data_manager.views AS b
          ON ( b.view_id = ANY(a.view_dependencies) )
        INNER JOIN public.fips_codes AS c
          ON ( b.geography_version = c.state_code )
      WHERE ( a.view_id = $1 )
  `);

  const { rows } = await dama_db.query({
    text: sql,
    values: [travel_times_dama_view_id],
  });

  return rows.length ? rows : [];
}

async function updateTmcDateRanges(
  auth_travel_times_imports_diff: NpmrdsTravelTimesImportInfo[]
) {
  logger.debug(JSON.stringify(auth_travel_times_imports_diff, null, 4));
  const seen_states = new Set();

  for (const {
    table_schema,
    table_name,
    state,
  } of auth_travel_times_imports_diff) {
    if (!seen_states.has(state)) {
      await create_state_tmc_date_ranges(state);
      seen_states.add(state);
    }

    const sql = dedent(
      pgFormat(
        `
          INSERT INTO %I.tmc_date_ranges AS a (tmc, first_date, last_date)
            SELECT
                b.tmc,
                MIN(b.date) AS first_date,
                MAX(b.date) AS last_date
              FROM %I.%I AS b
              GROUP BY tmc
            ON CONFLICT (tmc)
            DO UPDATE
              SET
                first_date  = LEAST(a.first_date, EXCLUDED.first_date),
                last_date   = GREATEST(a.last_date, EXCLUDED.last_date)
          ;
        `,
        state,
        table_schema,
        table_name
      )
    );

    await dama_db.query(sql);
  }

  for (const state of [...seen_states]) {
    const cluster_sql = pgFormat("CLUSTER %I.tmc_date_ranges ;", state);
    await dama_db.query(cluster_sql);
  }
}

async function computeStatistics() {
  const sql = dedent(`
    SELECT
        state,
        MIN(first_date)::TEXT as start_date,
        MAX(last_date)::TEXT as end_date
      FROM public.tmc_date_ranges
      GROUP BY 1
    ;
  `);

  const { rows } = await dama_db.query(sql);

  const data_date_extents_by_state = rows.reduce(
    (acc, { state, start_date, end_date }) => {
      acc[state] = [start_date, end_date];
      return acc;
    },
    {}
  );

  return { data_date_extents_by_state };
}

async function doit() {
  await initializeDamaSources();

  const new_travel_times_view_info = await getActiveTravelTimesViewInfo();

  // If there is no currently active NpmrdsTravelTimes, we're done.
  if (new_travel_times_view_info === null) {
    logger.warn("No authoritative NpmrdsTravelTimes DamaView");
    return;
  }

  const old_tmc_date_ranges_info = await getActiveTmcDateRangesInfo();

  const old_travel_times_dama_view_id =
    old_tmc_date_ranges_info?.old_travel_times_dama_view_id || null;

  const new_travel_times_dama_view_id = new_travel_times_view_info?.view_id;

  logger.debug(
    JSON.stringify(
      { old_travel_times_dama_view_id, new_travel_times_dama_view_id },
      null,
      4
    )
  );

  // If the current NpmrdsTmcDateRanges used the currently active NpmrdsTravelTimes, we're done.
  if (old_travel_times_dama_view_id === new_travel_times_dama_view_id) {
    logger.info("NpmrdsTmcDateRanges is up to date.");
    return;
  }

  // The active NpmrdsTravelTimesImports when NpmrdsTmcDateRanges was last updated.
  const old_auth_travel_times_imports = await getNpmrdsTravelTimesImportInfo(
    old_tmc_date_ranges_info?.old_travel_times_dama_view_id
  );

  // The currently active NpmrdsTravelTimesImports.
  const new_auth_travel_times_imports = await getNpmrdsTravelTimesImportInfo(
    new_travel_times_view_info?.view_id
  );

  console.log(
    JSON.stringify(
      { old_auth_travel_times_imports, new_auth_travel_times_imports },
      null,
      4
    )
  );

  // The NpmrdsTravelTimesImports added to NpmrdsTravelTimes since NpmrdsTmcDateRanges was last updated.
  const auth_travel_times_imports_diff = _.differenceWith(
    // @ts-ignore
    new_auth_travel_times_imports,
    old_auth_travel_times_imports,
    _.isEqual
  );

  const old_tmc_date_ranges_dama_view_id =
    old_tmc_date_ranges_info?.old_tmc_date_ranges_dama_view_id || null;

  await updateTmcDateRanges(auth_travel_times_imports_diff);

  const source_id = await getNpmrdsTmcDateRangesSourceId();
  const statistics = await computeStatistics();

  const now = new Date();

  const new_view_meta = {
    source_id,
    geography_version: new_travel_times_view_info.geography_version,
    table_schema: "public",
    table_name: "tmc_date_ranges",
    view_dependencies: [new_travel_times_view_info.view_id],
    statistics,
    metadata: {
      versionLinkedList: {
        previous: old_tmc_date_ranges_dama_view_id,
        next: null,
      },
    },
    start_date: new_travel_times_view_info.start_date,
    end_date: new_travel_times_view_info.end_date,
    active_start_timestamp: now,
  };

  const { view_id: new_tmc_date_ranges_dama_view_id } =
    await dama_meta.createNewDamaView(new_view_meta);

  const update_old_tmc_date_ranges_view_sql = dedent(`
    UPDATE data_manager.views
      SET
        metadata = jsonb_set(metadata, '{versionLinkedList, next}', $1::JSONB),
        active_end_timestamp = $2
      WHERE ( view_id = $3 )
    ;
  `);

  await dama_db.query({
    text: update_old_tmc_date_ranges_view_sql,
    values: [
      new_tmc_date_ranges_dama_view_id,
      now,
      old_tmc_date_ranges_dama_view_id,
    ],
  });

  return new_tmc_date_ranges_dama_view_id;
}

export default function main() {
  return dama_db.isInTransactionContext
    ? doit()
    : dama_db.runInTransactionContext(doit);
}
