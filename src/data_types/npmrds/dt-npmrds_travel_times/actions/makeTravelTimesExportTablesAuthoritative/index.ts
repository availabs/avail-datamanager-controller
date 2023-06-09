import dedent from "dedent";
import { DateTime } from "luxon";
import _ from "lodash";

import dama_db from "data_manager/dama_db";
import logger from "data_manager/logger";

import { NpmrdsDataSources, NpmrdsState } from "../../../domain";

import { EttViewsMetaSummary } from "./domain";

import getAttViewsToDetach from "./getAttViewsToDetach";

import getEttViewsMetadataSummary from "./getEttViewsMetadataSummary";

import {
  createAuthoritativeStateYearMonthTable,
  attachPartitionTable,
  detachAttView,
  updateNpmrdsAuthTravTimesViewMeta,
} from "./npmrdsAuthoritativeTravelTimesSql";

const { NpmrdsTravelTimes } = NpmrdsDataSources;

export type InitialEvent = {
  payload: { dama_view_ids: number[] };
};

function validateNpmrdsTravelTimesExportEligibleForAuthoritative(
  ettViewsMeta: EttViewsMetaSummary
) {
  const errorMessages: string[] = [];

  for (const ettViewId of ettViewsMeta.sortedByStateThenStartDate) {
    const ettViewMeta = ettViewsMeta.byViewId[ettViewId];

    const { damaViewId, table_name, state, is_expanded, start_date, end_date } =
      ettViewMeta;

    const startDateTime = DateTime.fromISO(start_date);
    const { year: dataStartYear, month: dataStartMonth } = startDateTime;

    const endDateTime = DateTime.fromISO(end_date);
    const { year: dataEndYear, month: dataEndMonth } = endDateTime;

    if (state === "ny" && !is_expanded) {
      errorMessages.push(
        `Authoritative NY TravelTimes MUST use the NPMRDS expanded map. ${table_name} (view_id ${damaViewId}) does not.`
      );
    }

    if (!(dataStartYear === dataEndYear && dataStartMonth === dataEndMonth)) {
      errorMessages.push(
        // eslint-disable-next-line max-len
        `${NpmrdsTravelTimes} tables MUST be within a single calendar month. The ${table_name} (view_id ${damaViewId}) is for ${start_date} to ${end_date}.`
      );
    }
  }

  if (errorMessages.length) {
    const errorsList = errorMessages.join("\n\t* ");
    const header = `INVARIANT${errorMessages.length > 1 ? "S" : ""} BROKEN`;
    throw new Error(`${header}:\n\t* ${errorsList}`);
  }
}

export async function getCurrentNpmrdsAuthoritativeTravelTimesViewMetadata() {
  const sql = dedent(`
    SELECT
        b.*
      FROM data_manager.sources AS a
        INNER JOIN data_manager.views AS b
          USING ( source_id )
      WHERE (
        ( a.name = $1 )
        AND
        -- The current NpmrdsTravelTimes doesn't have a next,
        --   indicating it's the tail of the LinkedList.
        ( b.metadata->'dama'->'versionLinkedList'->'next' = 'null'::JSONB )
      )
  `);

  const { rows } = await dama_db.query({
    text: sql,
    values: [NpmrdsTravelTimes],
  });

  if (rows.length === 0) {
    return null;
  }

  if (rows.length > 1) {
    throw new Error(
      `INVARIANT VIOLATION: More than one ${NpmrdsTravelTimes} DamaView with dama.versionLinkedList.nextViewId = null`
    );
  }

  return rows[0];
}

export default async function makeTravelTimesExportTablesAuthoritative(
  dama_view_ids: number[]
) {
  dama_view_ids = Array.isArray(dama_view_ids)
    ? dama_view_ids
    : [dama_view_ids];

  // const eventTypePrefix =
  // "dama/data_types/npmrds/dt-npmrds_travel_times.makeTravelTimesExportTablesAuthoritative";

  const fn = async () => {
    const curNpmrdsAuthTravTimesViewMeta =
      (await getCurrentNpmrdsAuthoritativeTravelTimesViewMetadata()) || {};

    const { view_dependencies: curAttViewIds = [] } =
      curNpmrdsAuthTravTimesViewMeta || {};

    const curAttViewIdsSet: Set<number> = new Set(curAttViewIds);

    // The currently non-ATT dama_view_ids
    const nonAttViewIds: number[] = dama_view_ids
      .map((viewId: number | string) => +viewId)
      .filter((viewId: number) => !curAttViewIdsSet.has(viewId));

    // If every submitted damaViewId is already authoritative, then no-op.
    if (nonAttViewIds.length === 0) {
      return [null, null];
    }

    const attViewsMeta = await getEttViewsMetadataSummary(curAttViewIds!);
    const ettViewsMeta = <EttViewsMetaSummary>(
      await getEttViewsMetadataSummary(nonAttViewIds)
    );

    validateNpmrdsTravelTimesExportEligibleForAuthoritative(ettViewsMeta);

    const { attViewsToDetach, dateExtentsByState } = getAttViewsToDetach(
      attViewsMeta,
      ettViewsMeta
    );

    for (const attView of attViewsToDetach) {
      await detachAttView(attView);
    }

    for (const ettViewId of ettViewsMeta.sortedByStateThenStartDate) {
      const ettViewMeta = ettViewsMeta.byViewId[ettViewId];

      const {
        table_schema: ettTableSchema,
        table_name: ettTableName,
        state,
        year,
        month,
        start_date,
        end_date,
      } = ettViewMeta;

      const dateRangeEnd = DateTime.fromISO(end_date)
        .plus({ days: 1 })
        .toISODate();

      const {
        table_schema: authoritativeSchemaName,
        table_name: authoritativeTableName,
      } = await createAuthoritativeStateYearMonthTable(
        <NpmrdsState>state,
        year,
        month
      );

      await attachPartitionTable(
        authoritativeSchemaName,
        authoritativeTableName,
        ettTableSchema,
        ettTableName,
        start_date,
        dateRangeEnd
      );
    }

    const newNpmrdsAuthTravTimesViewMeta =
      await updateNpmrdsAuthTravTimesViewMeta(
        curNpmrdsAuthTravTimesViewMeta,
        attViewsToDetach,
        attViewsMeta,
        ettViewsMeta,
        dateExtentsByState
      );

    // const done_data = {
    // oldDamaViewId: curNpmrdsAuthTravTimesViewMeta.view_id,
    // newDamaViewId: newNpmrdsAuthTravTimesViewMeta.view_id,
    // };

    // const finalEvent = {
    // type: `${eventTypePrefix}:FINAL`,
    // payload: finalEventPayload,
    // };

    // await dama_events.dispatch(finalEvent);

    const done_data = [
      curNpmrdsAuthTravTimesViewMeta,
      newNpmrdsAuthTravTimesViewMeta,
    ];

    logger.debug(JSON.stringify(done_data, null, 4));

    return done_data;
  };
  try {
    const [prev, cur] = dama_db.isInTransactionContext
      ? await fn()
      : await dama_db.runInTransactionContext(fn);

    return prev && cur
      ? {
          previousNpmrdsAuthTravTimesViewMeta: prev,
          curNpmrdsAuthTravTimesViewMeta: cur,
        }
      : null;
  } catch (err) {
    logger.error((<Error>err).message);
    logger.error((<Error>err).stack);

    // const errorEvent = {
    // type: `${eventTypePrefix}:ERROR`,
    // // @ts-ignore
    // payload: { message: err.message, stack: err.stack },
    // error: true,
    // };

    // await dama_events.dispatch(errorEvent);

    throw err;
  }
}
