import { join } from "path";

import dama_events from "data_manager/events";

import {
  getEtlContextId,
  getEtlWorkDir,
  verifyIsInTaskEtlContext,
} from "data_manager/contexts";
import BaseTasksController from "data_manager/tasks/BaseTasksController";
import { DamaTaskDescriptor } from "data_manager/tasks/domain";

import { TaskQueue, NpmrdsExportTransformOutput } from "../domain";

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

export default async function main(
  initial_event: InitialEvent
): Promise<DoneData> {
  verifyIsInTaskEtlContext();

  const events = await dama_events.getAllEtlContextEvents();

  let final_event = events.find(({ type }) => type === ":FINAL");

  if (final_event) {
    return final_event;
  }

  const task_controller = new BaseTasksController();

  const request_and_download_initial_event = {
    ...initial_event,
    meta: { note: "request and download export" },
  };

  const download_task_desc: DamaTaskDescriptor = {
    worker_path: download_worker_path,
    dama_task_queue_name: TaskQueue.DOWNLOAD_EXPORT,
    parent_context_id: getEtlContextId(),
    initial_event: request_and_download_initial_event,
    etl_work_dir: getEtlWorkDir(),
  };

  const { etl_context_id: download_eci } = await task_controller.queueDamaTask(
    download_task_desc,
    { retryLimit: 0, expireInHours: 24 * 7 }
  );

  const download_final_event = <DownloadFinalEvent>(
    await dama_events.getEventualEtlContextFinalEvent(download_eci)
  );

  const transform_initial_event = {
    type: ":INITIAL",
    payload: download_final_event.payload,
    meta: { note: "transform download" },
  };

  const transform_task_desc: DamaTaskDescriptor = {
    worker_path: transform_worker_path,
    dama_task_queue_name: TaskQueue.TRANSFORM_EXPORT,
    parent_context_id: getEtlContextId(),
    initial_event: transform_initial_event,
    etl_work_dir: getEtlWorkDir(),
  };

  const { etl_context_id: transform_eci } = await task_controller.queueDamaTask(
    transform_task_desc,
    { retryLimit: 0, expireInHours: 24 * 7 }
  );

  const transform_final_event = <TransformFinalEvent>(
    await dama_events.getEventualEtlContextFinalEvent(transform_eci)
  );

  const { payload: done_data } = transform_final_event;

  final_event = {
    type: ":FINAL",
    payload: done_data,
  };

  await dama_events.dispatch(final_event);

  return done_data;
}
