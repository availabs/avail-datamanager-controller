import { join } from "path";

import dama_events, { EtlEvent, DamaEvent } from "data_manager/events";

import {
  getEtlContextId,
  getEtlWorkDir,
  verifyIsInTaskEtlContext,
} from "data_manager/contexts";
import BaseTasksController from "data_manager/tasks/BaseTasksController";
import { DamaTaskDescriptor } from "data_manager/tasks/domain";

import {
  TaskQueue,
  NpmrdsExportRequest,
  NpmrdsExportTransformOutput,
} from "../domain";

import {
  InitialEvent as DownloadInitialEvent,
  FinalEvent as DownloadFinalEvent,
} from "./tasks/download";

import { FinalEvent as TransformFinalEvent } from "./tasks/transform";

const download_worker_path = join(__dirname, "./tasks/download/worker.ts");
const transform_worker_path = join(__dirname, "./tasks/transform/worker.ts");

export type DoneData = NpmrdsExportTransformOutput;

export type InitialEvent = DownloadInitialEvent;
export type FinalEvent = {
  type: ":FINAL";
  payload: DoneData;
};

enum SubtaskEventType {
  REQUEST_AND_DOWNLOAD_QUEUED = "REQUEST_AND_DOWNLOAD_QUEUED",
  TRANSFORM_QUEUED = "TRANSFORM_QUEUED",
}

async function download(
  npmrds_export_request: NpmrdsExportRequest,
  events: DamaEvent[],
  task_controller: BaseTasksController
) {
  let request_and_download_queued_event: EtlEvent | undefined = events.find(
    ({ type }) => type === SubtaskEventType.REQUEST_AND_DOWNLOAD_QUEUED
  );

  if (!request_and_download_queued_event) {
    const request_and_download_initial_event = {
      type: ":INITIAL",
      payload: npmrds_export_request,
      meta: { note: "request and download export" },
    };

    const download_task_desc: DamaTaskDescriptor = {
      worker_path: download_worker_path,
      dama_task_queue_name: TaskQueue.DOWNLOAD_EXPORT,
      parent_context_id: getEtlContextId(),
      initial_event: request_and_download_initial_event,
      etl_work_dir: getEtlWorkDir(),
    };

    const { etl_context_id: download_eci } =
      await task_controller.queueDamaTask(download_task_desc, {
        retryLimit: 0, // Because we don't want to make duplicate requests. TODO: implement a way to check.
        expireInHours: 24 * 7, // Because it may be deep in a queue.
      });

    request_and_download_queued_event = {
      type: SubtaskEventType.REQUEST_AND_DOWNLOAD_QUEUED,
      payload: {
        etl_context_id: download_eci,
      },
    };

    await dama_events.dispatch(request_and_download_queued_event);
  }

  const {
    payload: { etl_context_id: download_eci },
  } = <DamaEvent>request_and_download_queued_event;

  const download_final_event = <DownloadFinalEvent>(
    await dama_events.getEventualEtlContextFinalEvent(download_eci)
  );

  return download_final_event.payload;
}

async function transform(
  download_done_data: DownloadFinalEvent["payload"],
  events: DamaEvent[],
  task_controller: BaseTasksController
) {
  let transform_queued_event: EtlEvent | undefined = events.find(
    ({ type }) => type === SubtaskEventType.TRANSFORM_QUEUED
  );

  if (!transform_queued_event) {
    const transform_initial_event = {
      type: ":INITIAL",
      payload: download_done_data,
      meta: { note: "transform download" },
    };

    const transform_task_desc: DamaTaskDescriptor = {
      worker_path: transform_worker_path,
      dama_task_queue_name: TaskQueue.TRANSFORM_EXPORT,
      parent_context_id: getEtlContextId(),
      initial_event: transform_initial_event,
      etl_work_dir: getEtlWorkDir(),
    };

    const { etl_context_id } = await task_controller.queueDamaTask(
      transform_task_desc,
      {
        retryLimit: 0,
        expireInHours: 24 * 7,
      }
    );

    transform_queued_event = {
      type: SubtaskEventType.TRANSFORM_QUEUED,
      payload: {
        etl_context_id,
      },
    };

    await dama_events.dispatch(transform_queued_event);
  }

  const {
    payload: { etl_context_id },
  } = <DamaEvent>transform_queued_event;

  const transform_final_event = <TransformFinalEvent>(
    await dama_events.getEventualEtlContextFinalEvent(etl_context_id)
  );

  return transform_final_event.payload;
}

export default async function main(
  initial_event: InitialEvent
): Promise<DoneData> {
  verifyIsInTaskEtlContext();

  const events = await dama_events.getAllEtlContextEvents();

  let final_event = events.find(({ type }) => type === ":FINAL");

  if (final_event) {
    return final_event.payload;
  }

  const task_controller = new BaseTasksController();

  const { payload: npmrds_export_request } = initial_event;

  const download_done_data = await download(
    npmrds_export_request,
    events,
    task_controller
  );

  const transform_done_data = await transform(
    download_done_data,
    events,
    task_controller
  );

  final_event = {
    type: ":FINAL",
    payload: transform_done_data,
  };

  await dama_events.dispatch(final_event);

  return transform_done_data;
}
