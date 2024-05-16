/*
  SELECT
      a.*
    FROM data_manager.views AS a
      INNER JOIN data_manager.show_etl_context_tree(5452) AS b
        USING (etl_context_id)
    WHERE ( a.source_id = 93 )
    ORDER BY 1;
*/

import { join } from "path";

import _ from "lodash";
import { DateTime } from "luxon";

import dama_db from "data_manager/dama_db";
import dama_meta from "data_manager/meta";
import dama_events, { EtlEvent } from "data_manager/events";
import logger from "data_manager/logger";
import {
  verifyIsInTaskEtlContext,
  getEtlContextId,
} from "data_manager/contexts";

import doSubtask, { SubtaskConfig } from "data_manager/tasks/utils/doSubtask";
import { QueuedDamaTaskDescriptor } from "data_manager/tasks/domain";

import { stateAbbr2FipsCode } from "data_utils/constants/stateFipsCodes";
import dedent from "dedent";
import {
  NpmrdsState,
  NpmrdsDataSources,
  TaskQueue as NpmrdsTaskQueue,
  NpmrdsExportRequest,
} from "../domain";

import { InitialEvent as BatchedEtlInitialEvent } from "./batched-etl";

import MassiveDataDownloader from "../dt-npmrds_travel_times_export_ritis/puppeteer/MassiveDataDownloader";

export type InitialEvent = {
  type: ":INITIAL";
  payload?: {
    states: NpmrdsState[];
    is_expanded?: boolean;
    override_max_partitions?: boolean;
  } | null;
};

export enum EventType {
  "STATE_NPMRDS_EXPORT_REQUEST" = "STATE_NPMRDS_EXPORT_REQUEST",
}

const expected_date_format_re = /^\d{4}-\d{2}-\d{2}$/;

const batch_etl_worker_path = join(__dirname, "./batched-etl.worker.ts");

const default_states = [
  // NOTE: We iterate in order of smallest to largest requests so we know sooner if something fails.
  NpmrdsState.qc,
  NpmrdsState.on,
  // NpmrdsState.nj,
  // NpmrdsState.ny,
];

export async function getBatchExportRequest(
  source_id: number,
  state: NpmrdsState,
  max_data_date: string,
  is_expanded = true,
  override_max_partitions = false
): Promise<BatchedEtlInitialEvent["payload"] | null> {
  // CONSIDER: Maybe we should use the max date of imports actually used in the authoritative travel times.
  const max_date_for_state_sql = dedent(`
    SELECT
        MAX(end_date) AS max_loaded_date
      FROM data_manager.views
      WHERE (
        ( source_id = $1 )
        AND
        ( geography_version = $2 )
      )
  `);

  const fips_code = stateAbbr2FipsCode[state];

  const {
    rows: [{ max_loaded_date }],
  } = await dama_db.query({
    text: max_date_for_state_sql,
    values: [source_id, fips_code],
  });

  const next_date = DateTime.fromJSDate(max_loaded_date)
    .plus({ day: 1 })
    .toISODate();

  console.log("max_loaded_date:", max_loaded_date);
  console.log("max_data_date:", max_data_date);
  console.log("next_date:", next_date);

  if (!expected_date_format_re.test(next_date)) {
    throw new Error(
      `data_manager.views end_date not in expected YYY-MM-DD format: ${next_date}`
    );
  }

  if (next_date > max_data_date) {
    return null;
  }

  return {
    state,
    start_date: next_date,
    end_date: max_data_date,
    is_expanded,
    override_max_partitions,
  };
}

async function getStateNpmrdsExportRequests(events: EtlEvent[]) {
  const [initial_event] = events;

  const {
    states = default_states,
    is_expanded = true,
    override_max_partitions = false,
  } = (initial_event.payload as InitialEvent["payload"]) || {};

  let state_export_requests_event = events.find(
    ({ type }) => type === EventType.STATE_NPMRDS_EXPORT_REQUEST
  );

  if (state_export_requests_event) {
    return state_export_requests_event.payload;
  }

  const mdd = new MassiveDataDownloader();

  const [, max_data_date] = await mdd.getNpmrdsDataDateExtent();

  if (!expected_date_format_re.test(max_data_date)) {
    throw new Error(
      `MassiveDataDownloader npmrds data dates not in expected YYY-MM-DD format: ${max_data_date}`
    );
  }

  const source_id = await dama_meta.getDamaSourceIdForName(
    NpmrdsDataSources.NpmrdsTravelTimesImports
  );

  const state_npmrds_export_requests = await Promise.all(
    states
      .map((state) =>
        getBatchExportRequest(
          source_id,
          state,
          max_data_date,
          is_expanded,
          override_max_partitions
        )
      )
      .filter(Boolean)
  );

  state_export_requests_event = {
    type: EventType.STATE_NPMRDS_EXPORT_REQUEST,
    payload: state_npmrds_export_requests,
  };

  await dama_events.dispatch(state_export_requests_event);

  return state_export_requests_event.payload;
}

async function runStateBatchETLs(state_export_requests: NpmrdsExportRequest[]) {
  const events = await dama_events.getAllEtlContextEvents();

  const task_name = "runStateBatchETLs";

  const task_done_type = `${task_name}:DONE`;

  let task_done_event = events.find(({ type }) => type === task_done_type);

  if (task_done_event) {
    return task_done_event.payload;
  }

  const done_data = [] as EtlEvent[];

  for (const export_request of state_export_requests) {
    const { state } = export_request;

    const subtask_name = `batched-etl/${state}`;

    const subtask_queued_event_type = `${subtask_name}:QUEUED`;
    const subtask_done_event_type = `${subtask_name}:DONE`;

    await dama_events.dispatch({ type: `${subtask_name}:START` });

    const batch_initial_event: BatchedEtlInitialEvent = {
      type: ":INITIAL",
      payload: {
        ...export_request,
      },
      meta: { note: `Scheduled batch ETL for ${state}` },
    };

    const dama_task_descriptor: QueuedDamaTaskDescriptor = {
      worker_path: batch_etl_worker_path,
      dama_task_queue_name: NpmrdsTaskQueue.AGGREGATE_ETL,
      parent_context_id: getEtlContextId(),
      initial_event: batch_initial_event,
    };

    const subtask_config: SubtaskConfig = {
      subtask_name,
      dama_task_descriptor,
      subtask_queued_event_type,
      subtask_done_event_type,
    };

    const subtask_final_event = await doSubtask(subtask_config);

    done_data.push(subtask_final_event);
  }

  task_done_event = {
    type: task_done_type,
    payload: done_data,
  };

  await dama_events.dispatch(task_done_event);

  return task_done_event.payload;
}

export default async function main(initial_event: InitialEvent) {
  verifyIsInTaskEtlContext();

  const events = await dama_events.getAllEtlContextEvents();

  let final_event = events.find(({ type }) => type === ":FINAL");

  if (final_event) {
    return final_event.payload;
  }

  const state_export_requests = await getStateNpmrdsExportRequests(events);

  logger.debug(JSON.stringify({ state_export_requests }, null, 4));

  const done_data = await runStateBatchETLs(state_export_requests);

  final_event = {
    type: ":FINAL",
    payload: done_data,
  };

  await dama_events.dispatch(final_event);

  return done_data;
}
