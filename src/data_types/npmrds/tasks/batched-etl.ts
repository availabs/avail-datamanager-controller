import { join } from "path";

import _ from "lodash";
import dama_events from "data_manager/events";
import {
  verifyIsInTaskEtlContext,
  getEtlContextId,
} from "data_manager/contexts";
import logger from "data_manager/logger";

import doSubtask, { SubtaskConfig } from "data_manager/tasks/utils/doSubtask";
import { QueuedDamaTaskDescriptor } from "data_manager/tasks/domain";

import { TaskQueue as NpmrdsTaskQueue } from "../domain";

import { partitionDateRange } from "../utils/dates";

import { InitialEvent as AggregateEtlInitialEvent } from "./aggregate-etl";

export type InitialEvent = AggregateEtlInitialEvent & {
  payload: {
    override_max_paritions?: boolean;
  };
};

export enum EventType {
  "GOT_PARITIONS" = "GOT_PARITIONS",
}

const aggregate_etl_worker_path = join(__dirname, "./aggregate-etl.worker.ts");

// So we don't inadvertently request years of data from RITIS.
const MAX_DOWNLOAD_PARTITIONS_PER_STATE = 3;

export default async function main(initial_event: InitialEvent) {
  verifyIsInTaskEtlContext();

  const {
    payload: {
      state,
      start_date,
      end_date,
      is_expanded,
      override_max_paritions = false,
    },
  } = initial_event;

  const events = await dama_events.getAllEtlContextEvents();

  let final_event = events.find(({ type }) => type === ":FINAL");

  if (final_event) {
    return final_event.payload;
  }

  //  Partition the date range into complete months and complete weeks
  //    so the npmrds_travel_times imports fit into the database schema.
  const batches: AggregateEtlInitialEvent["payload"][] = partitionDateRange(
    start_date,
    end_date
  ).map((partition) => ({
    state,
    is_expanded,
    ...partition,
  }));

  // Limit the number of batches so we don't inadvertently request years of data from RITIS.
  if (
    batches.length > MAX_DOWNLOAD_PARTITIONS_PER_STATE &&
    !override_max_paritions
  ) {
    throw new Error(
      `ETL for ${state} ${start_date} to ${end_date} would spawn ${batches.length} subtasks, exceeding the max of ${MAX_DOWNLOAD_PARTITIONS_PER_STATE}.`
    );
  }

  logger.debug(JSON.stringify({ batches }, null, 4));

  const done_data = await Promise.all(
    batches.map(async (export_request) => {
      const { start_date: batch_start_date, end_date: batch_end_date } =
        export_request;

      const start = batch_start_date.replace(/-/g, "");
      const end = batch_end_date.replace(/-/g, "");

      const subtask_name = `${state}/${start}-${end}`;

      const task_initial_event: AggregateEtlInitialEvent = {
        type: ":INITIAL",
        payload: export_request,
        meta: {
          note: `Aggregate ETL for ${state} ${batch_start_date} to ${batch_end_date}`,
        },
      };

      const dama_task_descriptor: QueuedDamaTaskDescriptor = {
        worker_path: aggregate_etl_worker_path,
        dama_task_queue_name: NpmrdsTaskQueue.AGGREGATE_ETL,
        parent_context_id: getEtlContextId(),
        initial_event: task_initial_event,
      };

      const subtask_config: SubtaskConfig = {
        subtask_name,
        dama_task_descriptor,
        subtask_queued_event_type: `${subtask_name}:QUEUED`,
        subtask_done_event_type: `${subtask_name}:DONE`,
      };

      const { etl_context_id } = await doSubtask(subtask_config);

      return {
        etl_context_id,
        export_request,
      };
    })
  );

  final_event = {
    type: ":FINAL",
    payload: done_data,
  };

  await dama_events.dispatch(final_event);

  return done_data;
}
