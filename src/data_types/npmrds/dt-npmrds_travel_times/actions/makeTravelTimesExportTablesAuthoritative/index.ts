import dedent from "dedent";
import { DateTime } from "luxon";
import _ from "lodash";

import { Context } from "moleculer";

import { NpmrdsDataSources } from "../../../domain";

import { EttViewsMetaSummary, NodePgDbConnection } from "./domain";

import getAttViewsToDetach from "./getAttViewsToDetach";

import getEttViewsMetadataSummary from "./getEttViewsMetadataSummary";

import {
  createAuthoritativeStateYearMonthTable,
  attachPartitionTable,
  detachAttView,
  updateNpmrdsAuthTravTimesViewMeta,
} from "./npmrdsAuthoritativeTravelTimesSql";

const { NpmrdsTravelTimes } = NpmrdsDataSources;

function validateNpmrdsTravelTimesExportEligibleForAuthoritative(
  ettViewsMeta: EttViewsMetaSummary
) {
  const errorMessages: string[] = [];

  for (const ettViewId of ettViewsMeta.sortedByStateThenStartDate) {
    const ettViewMeta = ettViewsMeta.byViewId[ettViewId];

    const {
      damaViewId,
      tableName,
      state,
      isExpanded,
      data_start_date,
      data_end_date,
    } = ettViewMeta;

    const startDateTime = DateTime.fromISO(data_start_date);
    const { year: dataStartYear, month: dataStartMonth } = startDateTime;

    const endDateTime = DateTime.fromISO(data_end_date);
    const { year: dataEndYear, month: dataEndMonth } = endDateTime;

    if (state === "ny" && !isExpanded) {
      errorMessages.push(
        `Authoritative NY TravelTimes MUST use the NPMRDS expanded map. ${tableName} (view_id ${damaViewId}) does not.`
      );
    }

    if (!(dataStartYear === dataEndYear && dataStartMonth === dataEndMonth)) {
      errorMessages.push(
        // eslint-disable-next-line max-len
        `${NpmrdsTravelTimes} tables MUST be within a single calendar month. The ${tableName} (view_id ${damaViewId}) is for ${data_start_date} to ${data_end_date}.`
      );
    }
  }

  if (errorMessages.length) {
    const errorsList = errorMessages.join("\n\t* ");
    const header = `INVARIANT${errorMessages.length > 1 ? "S" : ""} BROKEN`;
    throw new Error(`${header}:\n\t* ${errorsList}`);
  }
}

export async function getCurrentNpmrdsAuthoritativeTravelTimesViewMetadata(
  dbConn: NodePgDbConnection
) {
  const sql = dedent(`
    -- SELECT FOR SHARE
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

  const { rows } = await dbConn.query({
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
  ctx: Context
) {
  let {
    // @ts-ignore
    params: { damaViewIds },
  } = ctx;

  damaViewIds = Array.isArray(damaViewIds) ? damaViewIds : [damaViewIds];

  const eventTypePrefix =
    "dama/data_types/npmrds/dt-npmrds_travel_times.makeTravelTimesExportTablesAuthoritative";

  const source_id = await ctx.call("dama/metadata.getDamaSourceIdForName", {
    damaSourceName: NpmrdsTravelTimes,
  });

  const etl_context_id: number = await ctx.call(
    "data_manager/events.spawnEtlContext",
    {
      source_id,
      // @ts-ignore
      parent_context_id: ctx.meta?.etl_context_id || null,
    }
  );

  const etlOpts = { parentCtx: ctx, meta: { etl_context_id } };

  const initialEvent = {
    type: `${eventTypePrefix}:INITIAL`,
    payload: { damaViewIds },
  };

  await ctx.call("data_manager/events.dispatch", initialEvent, etlOpts);

  const dbConn: NodePgDbConnection = await ctx.call("dama_db.getDbConnection");

  try {
    dbConn.query("BEGIN ;");

    const curNpmrdsAuthTravTimesViewMeta =
      (await getCurrentNpmrdsAuthoritativeTravelTimesViewMetadata(dbConn)) ||
      {};

    const { view_dependencies: curAttViewIds = [] } =
      curNpmrdsAuthTravTimesViewMeta || {};

    const curAttViewIdsSet: Set<number> = new Set(curAttViewIds);

    // The currently non-ATT damaViewIds
    const nonAttViewIds: number[] = damaViewIds
      .map((viewId: number | string) => +viewId)
      .filter((viewId: number) => !curAttViewIdsSet.has(viewId));

    // If every submitted damaViewId is already authoritative, then no-op.
    if (nonAttViewIds.length === 0) {
      dbConn.query("COMMIT ;");
      return;
    }

    const attViewsMeta = await getEttViewsMetadataSummary(
      dbConn,
      curAttViewIds
    );
    const ettViewsMeta = <EttViewsMetaSummary>(
      await getEttViewsMetadataSummary(dbConn, nonAttViewIds)
    );

    validateNpmrdsTravelTimesExportEligibleForAuthoritative(ettViewsMeta);

    const { attViewsToDetach, dateExtentsByState } = getAttViewsToDetach(
      attViewsMeta,
      ettViewsMeta
    );

    for (const attView of attViewsToDetach) {
      await detachAttView(dbConn, attView);
    }

    for (const ettViewId of ettViewsMeta.sortedByStateThenStartDate) {
      const ettViewMeta = ettViewsMeta.byViewId[ettViewId];

      const {
        tableSchema: ettTableSchema,
        tableName: ettTableName,
        state,
        year,
        month,
        data_start_date,
        data_end_date,
      } = ettViewMeta;

      const dateRangeEnd = DateTime.fromISO(data_end_date)
        .plus({ days: 1 })
        .toISODate();

      const {
        schemaName: authoritativeSchemaName,
        tableName: authoritativeTableName,
      } = await createAuthoritativeStateYearMonthTable(
        dbConn,
        state,
        year,
        month
      );

      await attachPartitionTable(
        dbConn,
        authoritativeSchemaName,
        authoritativeTableName,
        ettTableSchema,
        ettTableName,
        data_start_date,
        dateRangeEnd
      );
    }

    const newNpmrdsAuthTravTimesViewMeta =
      await updateNpmrdsAuthTravTimesViewMeta(
        dbConn,
        curNpmrdsAuthTravTimesViewMeta,
        attViewsToDetach,
        attViewsMeta,
        ettViewsMeta,
        dateExtentsByState,
        etl_context_id
      );

    const finalEventPayload = {
      oldDamaViewId: curNpmrdsAuthTravTimesViewMeta.view_id,
      newDamaViewId: newNpmrdsAuthTravTimesViewMeta.view_id,
    };

    const finalEvent = {
      type: `${eventTypePrefix}:FINAL`,
      payload: finalEventPayload,
    };

    await ctx.call("data_manager/events.dispatch", finalEvent, etlOpts);

    dbConn.query("COMMIT ;");

    return {
      previousNpmrdsAuthTravTimesViewMeta: curNpmrdsAuthTravTimesViewMeta,
      curNpmrdsAuthTravTimesViewMeta: newNpmrdsAuthTravTimesViewMeta,
    };
  } catch (err) {
    const errorEvent = {
      type: `${eventTypePrefix}:ERROR`,
      // @ts-ignore
      payload: { message: err.message },
      error: true,
    };

    await ctx.call("data_manager/events.dispatch", errorEvent, etlOpts);

    console.error(err);
    dbConn.query("ROLLBACK ;");
    throw err;
  } finally {
    // @ts-ignore
    if (dbConn.release) {
      // @ts-ignore
      await dbConn.release();
    }
  }
}