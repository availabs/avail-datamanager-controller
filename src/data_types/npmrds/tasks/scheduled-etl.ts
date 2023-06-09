import { join } from "path";

import _ from "lodash";
import { DateTime } from "luxon";

import dama_db from "data_manager/dama_db";
import dama_meta from "data_manager/meta";
import dama_events from "data_manager/events";
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
  payload?: { states: NpmrdsState[] } | null;
};

export enum EventType {
  "GOT_STATE_NPMRDS_EXPORT_REQUESTS" = "GOT_STATE_NPMRDS_EXPORT_REQUESTS",
}

const batch_etl_worker_path = join(__dirname, "./batched-etl.worker.ts");

const default_states = [
  NpmrdsState.ny,
  NpmrdsState.nj,
  NpmrdsState.on,
  NpmrdsState.qc,
];

export default async function main(initial_event: InitialEvent) {
  verifyIsInTaskEtlContext();

  const { states = default_states } = initial_event.payload || {};

  const events = await dama_events.getAllEtlContextEvents();

  let final_event = events.find(({ type }) => type === ":FINAL");

  if (final_event) {
    return final_event.payload;
  }

  let got_state_export_requests_event = events.find(
    ({ type }) => type === EventType.GOT_STATE_NPMRDS_EXPORT_REQUESTS
  );

  if (!got_state_export_requests_event) {
    const mdd = new MassiveDataDownloader();

    const expected_date_format_re = /^\d{4}-\d{2}-\d{2}$/;

    const [, max_data_date] = await mdd.getNpmrdsDataDateExtent();

    if (!expected_date_format_re.test(max_data_date)) {
      throw new Error(
        `MassiveDataDownloader npmrds data dates not in expected YYY-MM-DD format: ${max_data_date}`
      );
    }

    const source_id = await dama_meta.getDamaSourceIdForName(
      NpmrdsDataSources.NpmrdsTravelTimesImports
    );

    const state_npmrds_export_requests = <NpmrdsExportRequest[]>_.flatten(
      await Promise.all(
        states.map(async (state) => {
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

          if (!expected_date_format_re.test(next_date)) {
            throw new Error(
              `data_manager.views end_date not in expected YYY-MM-DD format: ${next_date}`
            );
          }

          if (next_date >= max_data_date) {
            return null;
          }

          return {
            state,
            start_date: next_date,
            end_date: max_data_date,
            // is_expanded: true,
            is_expanded: false, // FIXME: For development ONLY
          };
        })
      )
    ).filter(Boolean);

    got_state_export_requests_event = {
      type: EventType.GOT_STATE_NPMRDS_EXPORT_REQUESTS,
      payload: state_npmrds_export_requests,
    };

    await dama_events.dispatch(got_state_export_requests_event);
  }

  logger.debug(JSON.stringify({ got_state_export_requests_event }, null, 4));

  const done_data = await Promise.all(
    got_state_export_requests_event.payload.map(
      (export_request: NpmrdsExportRequest) => {
        const { state } = export_request;

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
          subtask_name: state,
          dama_task_descriptor,
          subtask_queued_event_type: `${state}:QUEUED`,
          subtask_done_event_type: `${state}:DONE`,
        };

        return doSubtask(subtask_config);
      }
    )
  );

  final_event = {
    type: ":FINAL",
    payload: done_data,
  };

  await dama_events.dispatch(final_event);

  return done_data;
}
