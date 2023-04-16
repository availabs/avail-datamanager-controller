import { inspect } from "util";
import { join } from "path";

import _ from "lodash";

import dama_events from "data_manager/events";
import logger from "data_manager/logger";

import BaseTasksController from "data_manager/tasks/BaseTasksController";
import { DamaTaskDescriptor } from "data_manager/tasks/domain";

import {
  getEtlContextId,
  verifyIsInTaskEtlContext,
} from "data_manager/contexts";

import getEtlWorkDir from "./utils/etlWorkDir";

import { InitialEvent as CollectEventIdsIntialEvent } from "./tasks/collect_transcom_event_ids";
import { InitialEvent as DownloadEventsInitialEvent } from "./tasks/download_transcom_events";
import { InitialEvent as LoadEventsInitialEvent } from "./tasks/load_transcom_events";
import { InitialEvent as GenerateDerivativeDataInitialEvent } from "./tasks/generate_derivative_datasources";
import { InitialEvent as PublishInitialEvent } from "./tasks/publish";

export enum TaskEventType {
  COLLECT_EVENT_IDS_QUEUED = ":COLLECT_EVENT_IDS_QUEUED",
  COLLECT_EVENT_IDS_DONE = ":COLLECT_EVENT_IDS_DONE",

  DOWNLOAD_EVENTS_QUEUED = ":DOWNLOAD_EVENTS_QUEUED",
  DOWNLOAD_EVENTS_DONE = ":DOWNLOAD_EVENTS_DONE",

  LOAD_EVENTS_QUEUED = ":LOAD_EVENTS_QUEUED",
  LOAD_EVENTS_DONE = ":LOAD_EVENTS_DONE",

  GENERATE_DERIVATIVE_DATA_QUEUED = ":GENERATE_DERIVATIVE_DATA_QUEUED",
  GENERATE_DERIVATIVE_DATA_DONE = ":GENERATE_DERIVATIVE_DATA_DONE",

  PUBLISH_QUEUED = ":PUBLISH_QUEUED",
  PUBLISH_DONE = ":PUBLISH_DONE",
}

export type InitialEvent = {
  type: ":INITIAL";
  payload: {
    start_timestamp: string;
    end_timestamp: string;
  };
};

export const dama_task_queue_name = "dt-transcom_events:subtasks";

const tasks_controller = new BaseTasksController();

export type SubtaskConfig = {
  subtask_name: string;
  dama_task_descriptor: DamaTaskDescriptor;
  subtask_queued_event_type: TaskEventType;
  subtask_done_event_type: TaskEventType;
};

export async function doSubtask(config: SubtaskConfig) {
  const {
    subtask_name,
    dama_task_descriptor,
    subtask_queued_event_type,
    subtask_done_event_type,
  } = config;

  const events = await dama_events.getAllEtlContextEvents();

  logger.debug(`${subtask_name} events: ${JSON.stringify(events, null, 4)}`);

  const done_event = events.find((e) => e.type === subtask_done_event_type);

  // If the done_event already has been emitted for this subtask, return it.
  if (done_event) {
    logger.debug(`${subtask_name} already DONE`);

    return done_event;
  }

  let queued_event = events.find((e) => e.type === subtask_queued_event_type);

  //  If the queued_event has not already has been emitted for this subtask,
  //    we must queue the subtask.
  if (!queued_event) {
    logger.debug(`${subtask_name} must queue subtask`);

    // @ts-ignore FIXME
    const { etl_context_id: eci } = await tasks_controller.queueDamaTask(
      dama_task_descriptor
    );

    logger.debug(`${subtask_name} queued subtask etl_context_id=${eci}`);

    queued_event = {
      type: subtask_queued_event_type,
      payload: {
        subtask_name,
        etl_context_id: eci,
      },
    };

    logger.debug(
      `${subtask_name} dispatching queued_event ${inspect(queued_event)}`
    );

    await dama_events.dispatch(queued_event);
  }

  const {
    payload: { etl_context_id: subtask_eci },
  } = queued_event;

  logger.debug(`${subtask_name} awaiting subtask :FINAL event`);

  const final_event = await dama_events.getEventualEtlContextFinalEvent(
    subtask_eci
  );

  logger.debug(`${subtask_name} received subtask :FINAL event`);

  await dama_events.dispatch({
    type: subtask_done_event_type,
    payload: { etl_context_id: subtask_eci },
  });

  return final_event;
}

export async function collectTranscomEventIdsForTimeRange(
  start_timestamp: string,
  end_timestamp: string
) {
  const subtask_name = "collect_transcom_event_ids";

  const worker_path = join(
    __dirname,
    "./tasks/collect_transcom_event_ids/worker.ts"
  );

  const initial_event: CollectEventIdsIntialEvent = {
    type: ":INITIAL",
    payload: {
      etl_work_dir: getEtlWorkDir(),
      start_timestamp,
      end_timestamp,
    },
    meta: { subtask_name },
  };

  const dama_task_descriptor: DamaTaskDescriptor = {
    worker_path,
    dama_task_queue_name,
    parent_context_id: getEtlContextId(),
    // source_id: // TODO
    initial_event,
  };

  const subtask_config: SubtaskConfig = {
    subtask_name,
    dama_task_descriptor,
    subtask_queued_event_type: TaskEventType.COLLECT_EVENT_IDS_QUEUED,
    subtask_done_event_type: TaskEventType.COLLECT_EVENT_IDS_DONE,
  };

  return doSubtask(subtask_config);
}

export async function downloadTranscomEvents() {
  const subtask_name = "download_transcom_events";

  const worker_path = join(
    __dirname,
    "./tasks/download_transcom_events/worker.ts"
  );

  const initial_event: DownloadEventsInitialEvent = {
    type: ":INITIAL",
    payload: {
      etl_work_dir: getEtlWorkDir(),
    },
    meta: { subtask_name },
  };

  const dama_task_descriptor: DamaTaskDescriptor = {
    worker_path,
    dama_task_queue_name,
    parent_context_id: getEtlContextId(),
    // source_id: // TODO
    initial_event,
  };

  const subtask_config: SubtaskConfig = {
    subtask_name,
    dama_task_descriptor,
    subtask_queued_event_type: TaskEventType.DOWNLOAD_EVENTS_QUEUED,
    subtask_done_event_type: TaskEventType.DOWNLOAD_EVENTS_DONE,
  };

  return doSubtask(subtask_config);
}

export async function loadTranscomEvents() {
  const subtask_name = "load_transcom_events";

  const worker_path = join(__dirname, "./tasks/load_transcom_events/worker.ts");

  const initial_event: LoadEventsInitialEvent = {
    type: ":INITIAL",
    payload: {
      etl_work_dir: getEtlWorkDir(),
    },
    meta: { subtask_name },
  };

  const dama_task_descriptor: DamaTaskDescriptor = {
    worker_path,
    dama_task_queue_name,
    parent_context_id: getEtlContextId(),
    // source_id: // TODO
    initial_event,
  };

  const subtask_config: SubtaskConfig = {
    subtask_name,
    dama_task_descriptor,
    subtask_queued_event_type: TaskEventType.LOAD_EVENTS_QUEUED,
    subtask_done_event_type: TaskEventType.LOAD_EVENTS_DONE,
  };

  return doSubtask(subtask_config);
}

export async function generateDerivativeData() {
  const subtask_name = "generate_derivative_datasources";

  const worker_path = join(
    __dirname,
    "./tasks/generate_derivative_datasources/worker.ts"
  );

  const initial_event: GenerateDerivativeDataInitialEvent = {
    type: ":INITIAL",
    payload: {
      etl_work_dir: getEtlWorkDir(),
    },
    meta: { subtask_name },
  };

  const dama_task_descriptor: DamaTaskDescriptor = {
    worker_path,
    dama_task_queue_name,
    parent_context_id: getEtlContextId(),
    // source_id: // TODO
    initial_event,
  };

  const subtask_config: SubtaskConfig = {
    subtask_name,
    dama_task_descriptor,
    subtask_queued_event_type: TaskEventType.GENERATE_DERIVATIVE_DATA_QUEUED,
    subtask_done_event_type: TaskEventType.GENERATE_DERIVATIVE_DATA_DONE,
  };

  return doSubtask(subtask_config);
}

export async function publish() {
  const worker_path = join(__dirname, "./tasks/publish/worker.ts");

  const initial_event: PublishInitialEvent = {
    type: ":INITIAL",
    payload: {
      etl_work_dir: getEtlWorkDir(),
    },
  };

  const dama_task_descriptor: DamaTaskDescriptor = {
    worker_path,
    dama_task_queue_name,
    parent_context_id: getEtlContextId(),
    // source_id: // TODO
    initial_event,
  };

  const subtask_config: SubtaskConfig = {
    subtask_name: "publish",
    dama_task_descriptor,
    subtask_queued_event_type: TaskEventType.PUBLISH_QUEUED,
    subtask_done_event_type: TaskEventType.PUBLISH_DONE,
  };

  return doSubtask(subtask_config);
}

export default async function main(initial_event: InitialEvent) {
  verifyIsInTaskEtlContext();

  logger.debug(`starting ${new Date().toISOString()}`);

  const {
    payload: { start_timestamp, end_timestamp },
  } = initial_event;

  const workflow = [
    collectTranscomEventIdsForTimeRange.bind(
      null,
      start_timestamp,
      end_timestamp
    ),
    downloadTranscomEvents,
    loadTranscomEvents,
    generateDerivativeData,
    publish,
  ];

  for (const subtask of workflow) {
    logger.debug(`beginning subtask ${subtask.name}`);

    await subtask();

    logger.debug(`finished subtask ${subtask.name}`);
  }

  return {
    type: ":FINAL",
    payload: { etl_work_dir: getEtlWorkDir() },
  };
}
