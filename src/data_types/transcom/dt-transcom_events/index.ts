import { inspect } from "util";
import { mkdirSync } from "fs";
import { join } from "path";

import _ from "lodash";

import dama_events from "data_manager/events";
import logger from "data_manager/logger";

import BaseTasksController from "data_manager/tasks/BaseTasksController";
import { DamaTaskDescriptor } from "data_manager/tasks/domain";

import etl_dir from "constants/etlDir";

import {
  getPgEnv,
  getEtlContextId,
  verifyIsInTaskEtlContext,
} from "data_manager/contexts";

import { InitialEvent as CollectEventIdsIntialEvent } from "./tasks/collect_transcom_event_ids";
import { InitialEvent as DownloadEventsInitialEvent } from "./tasks/dowload_transcom_events";

export enum TaskEventType {
  SPAWNED_COLLECT_EVENT_IDS = ":SPAWNED_COLLECT_EVENT_IDS",
  COLLECT_EVENT_IDS_DONE = ":COLLECT_EVENT_IDS_DONE",

  SPAWNED_DOWNLOAD_EVENTS = ":SPAWNED_DOWNLOAD_EVENTS",
  DOWNLOAD_EVENTS_DONE = ":DOWNLOAD_EVENTS_DONE",
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

export const getEtlWorkDir = () => {
  const etl_work_dir = join(
    etl_dir,
    `dt-transcom_events.${getPgEnv()}.${getEtlContextId()}`
  );

  mkdirSync(etl_work_dir, { recursive: true });

  return etl_work_dir;
};

export type SubtaskConfig = {
  subtask_name: string;
  dama_task_descriptor: DamaTaskDescriptor;
  subtask_spawned_event_type: TaskEventType;
  subtask_done_event_type: TaskEventType;
};

export async function doSubtask(config: SubtaskConfig) {
  const {
    subtask_name,
    dama_task_descriptor,
    subtask_spawned_event_type,
    subtask_done_event_type,
  } = config;

  const events = await dama_events.getAllEtlContextEvents();

  logger.debug(`${subtask_name} events: ${JSON.stringify(events, null, 4)}`);

  const done_event = events.find((e) => e.type === subtask_done_event_type);

  if (done_event) {
    logger.debug(`${subtask_name} already DONE`);

    const {
      payload: { etl_context_id: eci },
    } = done_event;

    return dama_events.getEtlContextFinalEvent(eci);
  }

  let spawned_event = events.find((e) => e.type === subtask_spawned_event_type);

  if (!spawned_event) {
    logger.debug(`${subtask_name} must spawn  subtask`);

    // @ts-ignore FIXME
    const { etl_context_id: eci } = await tasks_controller.queueDamaTask(
      dama_task_descriptor
    );

    logger.debug(`${subtask_name} queued subtask etl_context_id=${eci}`);

    spawned_event = {
      type: subtask_spawned_event_type,
      payload: {
        subtask_name,
        etl_context_id: eci,
      },
    };

    logger.debug(
      `${subtask_name} dispatching spawned_event ${inspect(spawned_event)}`
    );

    await dama_events.dispatch(spawned_event);
  }

  const {
    payload: { etl_context_id: subtask_eci },
  } = spawned_event;

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
  };

  const dama_task_descriptor: DamaTaskDescriptor = {
    worker_path,
    dama_task_queue_name,
    parent_context_id: getEtlContextId(),
    // source_id: // TODO
    initial_event,
  };

  const subtask_config: SubtaskConfig = {
    subtask_name: "collect_transcom_event_ids",
    dama_task_descriptor,
    subtask_spawned_event_type: TaskEventType.SPAWNED_COLLECT_EVENT_IDS,
    subtask_done_event_type: TaskEventType.COLLECT_EVENT_IDS_DONE,
  };

  return doSubtask(subtask_config);
}

export async function downloadTranscomEvents() {
  const worker_path = join(
    __dirname,
    "./tasks/dowload_transcom_events/worker.ts"
  );

  const initial_event: DownloadEventsInitialEvent = {
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
    subtask_name: "dowload_transcom_events",
    dama_task_descriptor,
    subtask_spawned_event_type: TaskEventType.SPAWNED_DOWNLOAD_EVENTS,
    subtask_done_event_type: TaskEventType.DOWNLOAD_EVENTS_DONE,
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
  ];

  for (const subtask of workflow) {
    logger.debug(`beginning subtask ${subtask.name}`);

    await subtask();

    logger.debug(`finished subtask ${subtask.name}`);
  }

  return { type: ":FINAL", payload: { etl_work_dir: getEtlWorkDir() } };
}
