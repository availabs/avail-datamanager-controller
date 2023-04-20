import { ExecaChildProcess } from "execa";
import { Job, Worker } from "pg-boss";

export type DamaTaskQueueName = Worker["name"];
export type DamaTaskId = Job["id"];

type InitialEvent = {
  type: string;
  payload?: any;
  meta?: any | null;
};

export type QueuedDamaTaskDescriptor = {
  worker_path: string;
  dama_task_queue_name?: DamaTaskQueueName;
  parent_context_id?: number | null;
  source_id?: number | null;
  initial_event: InitialEvent;
};

export type ScheduledDamaTaskDescriptor = QueuedDamaTaskDescriptor & {
  cron: string;
};

export type DamaTaskDescriptor =
  | QueuedDamaTaskDescriptor
  | ScheduledDamaTaskDescriptor;

export type DamaTaskMetadata = DamaTaskDescriptor & {
  task_id: DamaTaskId | null; // null if task not yet started
  etl_context_id: number;
  initial_event: DamaTaskDescriptor["initial_event"] & {
    event_id: number;
    etl_context_id: number;
  };
};

//  For QueuedDamaTasks, we create the EtlContext and dispatch the :INITIAL event
//    before sending the task to pg-boss.
export type QueuedJobData = {
  etl_context_id: number;
  worker_path: string;
};

//  For ScheduledDamaTasks, we cannot create the EtlContext and dispatch the :INITIAL event ahead of time.
//    Therefore we must create the EtlContext and dispatch the :INITIAL event each time an instance
//    of the Task is started by pg-boss.
export type ScheduledJobData = {
  initial_event: InitialEvent;
  source_id: number;
  worker_path: string;
};

export type QueuedDamaTaskJob = Job & {
  data: {
    etl_context_id: number;
    worker_path: string;
  };
};

export type ScheduledDamaTaskJob = Job & {
  data: {
    initial_event: InitialEvent;
    worker_path: string;
  };
};

export type DamaTaskJob = QueuedDamaTaskJob | ScheduledDamaTaskJob;

export type DamaTask = {
  etl_context_id: number;
  worker_path: string;
  task_id: string;
  task_process: ExecaChildProcess | null;
};

export enum DamaTaskExitCodes {
  DONE = 0,
  COULD_NOT_ACQUIRE_INITIAL_EVENT_LOCK = 100,
  WORKER_THREW_ERROR = 101,
  WORKER_DID_NOT_RETURN_FINAL_EVENT = 102,
}
