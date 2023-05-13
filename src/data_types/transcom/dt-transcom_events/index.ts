import { inspect } from "util";
import { join } from "path";

import _ from "lodash";
import dedent from "dedent";
import { DateTime, Interval } from "luxon";

import dama_db from "data_manager/dama_db";
import dama_events from "data_manager/events";
import logger from "data_manager/logger";

import doSubtask from "data_manager/tasks/utils/doSubtask";
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
    start_timestamp?: string;
    end_timestamp?: string;
  };
};

export const dama_task_queue_name = "dt-transcom_events:subtasks";

export type SubtaskConfig = {
  subtask_name: string;
  dama_task_descriptor: DamaTaskDescriptor;
  subtask_queued_event_type: TaskEventType;
  subtask_done_event_type: TaskEventType;
};

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

async function getStartTimestamp(): Promise<string> {
  const events_table_exists_sql = dedent(
    `
      SELECT EXISTS (
        SELECT
            1
          FROM pg_catalog.pg_tables
          WHERE (
            ( schemaname = '_transcom_admin' )
            AND
            ( tablename = 'transcom_events_expanded' )
          )
      ) AS events_table_exists ;
    `
  );

  const {
    rows: [{ events_table_exists }],
  } = await dama_db.query(events_table_exists_sql);

  if (events_table_exists) {
    try {
      const sql = dedent(
        `
          SELECT
              to_char(
                GREATEST (
                  -- If table is empty, start with this timestamp.
                  '2016-01-01T00:00'::TIMESTAMP,

                  --  Otherwise, use the current max start_date_time's previous day start.
                  --    EG: '2023-01-01T12:00:00' becomes '2022-12-31T00:00:00'
                  --  We do this because TRANSCOM revises event data.
                  (
                    DATE_TRUNC(
                      'day',
                      MAX(start_date_time)
                    ) - '1 day'::INTERVAL
                  )

                  -- For development/debugging
                  -- MAX(start_date_time) - '5 minutes'::INTERVAL
                ),
                'YYYY-MM-DD"T"HH24:MI:SS'
              ) AS latest_start_time
            FROM _transcom_admin.transcom_events_expanded
          ;
        `
      );

      const {
        rows: [{ latest_start_time }],
      } = await dama_db.query(sql);

      return latest_start_time;
    } catch (err) {
      logger.error(
        "ERROR: Could not connect to retreive the latest event from the database."
      );

      throw err;
    }
  }

  return "2016-01-01T00:00:00";
}

export default async function main(initial_event: InitialEvent) {
  verifyIsInTaskEtlContext();

  logger.debug(`starting ${new Date().toISOString()}`);

  const payload = initial_event.payload || {};

  let { start_timestamp, end_timestamp } = payload;

  // The nightly ETL does not specify either start_timestamp or end_timestamp.
  //   Instead it resumes from the latest event in the db.
  if (!start_timestamp) {
    start_timestamp = await getStartTimestamp();
  }

  if (!end_timestamp) {
    // ===== for production =====
    end_timestamp = DateTime.now().toISO().replace(/\..*/, "");
  }

  const interval = Interval.fromISO(`${start_timestamp}/${end_timestamp}`);
  if (interval.length("minutes") < 10) {
    const message =
      "Skipping TRANSCOM Events ETL. start_timestamp/end_timestamp interval less than 10 minutes.";

    logger.info(message);

    await dama_events.dispatch({ type: ":MESSAGE", payload: { message } });

    const fin = { type: ":FINAL" };

    await dama_events.dispatch(fin);

    return fin;
  }

  logger.debug(
    `start_timestamp=${start_timestamp}; end_timestamp=${end_timestamp}`
  );

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
