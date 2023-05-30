import { mkdirSync, linkSync, existsSync } from "fs";

import { join, dirname } from "path";

import dedent from "dedent";
import _ from "lodash";

import dama_db from "data_manager/dama_db";
import dama_events from "data_manager/events";
import logger from "data_manager/logger";

import {
  EtlContextId,
  getEtlContextId,
  verifyIsInTaskEtlContext,
} from "data_manager/contexts";

import { QueuedDamaTaskDescriptor } from "data_manager/tasks/domain";

import doSubtask, { SubtaskConfig } from "data_manager/tasks/utils/doSubtask";

import getEtlWorkDir from "var/getEtlWorkDir";

import { stateAbbr2FipsCode } from "data_utils/constants/stateFipsCodes";

import getEtlMetadataDir from "../dt-npmrds_travel_times_export_ritis/utils/getEtlMetadataDir";

import {
  getNpmrdsExportMetadataFilePath,
  getNpmrdsExportMetadata,
} from "data_types/npmrds/utils/npmrds_export_metadata";

// import dama_host_name from "constants/damaHost";
import dama_host_id from "constants/damaHostId";

import controllerDataDir from "constants/dataDir";
import npmrds_data_dir from "../constants/data_dir";

import { initializeDamaSources } from "../utils/dama_sources";

import {
  NpmrdsDataSources,
  NpmrdsTravelTimesExportRitisElements,
  NpmrdsTravelTimesExportEtlElements,
  NpmrdsExportRequest,
  TaskQueue as NpmrdsTaskQueue,
  NpmrdsExportMetadata,
} from "../domain";

import {
  InitialEvent as NpmrdsExportIntitialEvent,
  FinalEvent as NpmrdsExportFinalEvent,
} from "../dt-npmrds_travel_times_export_ritis";

import {
  InitialEvent as LoadTmcIdentificationIntialEvent,
  FinalEvent as LoadTmcIdentificationFinalEvent,
} from "../dt-npmrds_tmc_identification_imp";

import {
  InitialEvent as LoadNpmrdsTravelTimesInitialEvent,
  FinalEvent as LoadNpmrdsTravelTimesFinalEvent,
} from "../dt-npmrds_travel_times_imp";

import { DamaView } from "data_manager/meta/domain";

type EtlDoneData = {
  [NpmrdsDataSources.NpmrdsTravelTimesExportRitis]: {
    metadata: {
      files: {
        [NpmrdsTravelTimesExportRitisElements.NpmrdsAllVehiclesTravelTimesExport]: string;
        [NpmrdsTravelTimesExportRitisElements.NpmrdsPassengerVehiclesTravelTimesExport]: string;
        [NpmrdsTravelTimesExportRitisElements.NpmrdsFreightTrucksTravelTimesExport]: string;
      };
    };
  };
  [NpmrdsDataSources.NpmrdsTravelTimesExportEtl]: {
    metadata: {
      files: {
        [NpmrdsTravelTimesExportEtlElements.NpmrdsTravelTimesExportSqlite]: string;
        [NpmrdsTravelTimesExportEtlElements.NpmrdsTmcIdentificationCsv]: string;
        [NpmrdsTravelTimesExportEtlElements.NpmrdsTravelTimesCsv]: string;
      };
    };
  };

  [NpmrdsDataSources.NpmrdsTmcIdentificationImports]: LoadTmcIdentificationFinalEvent["payload"];

  [NpmrdsDataSources.NpmrdsTravelTimesImports]: LoadNpmrdsTravelTimesFinalEvent["payload"];
};

type IntegrateDoneData = DamaView[];

export type InitialEvent = {
  type: ":INITIAL";
  payload: NpmrdsExportRequest;
  meta?: object;
};

export type DoneData = {
  etl_context_id: EtlContextId;
  npmrds_export_metadata: NpmrdsExportMetadata;
  etl_done_data: EtlDoneData;
  integrate_done_data: IntegrateDoneData;
};

export type FinalEvent = {
  type: ":FINAL";
  payload: DoneData;
};

export enum TaskEventType {
  DOWNLOAD_AND_TRANSFORM_QUEUED = ":DOWNLOAD_AND_TRANSFORM_QUEUED",
  DOWNLOAD_AND_TRANSFORM_DONE = ":DOWNLOAD_AND_TRANSFORM_DONE",

  LOAD_TMC_IDENTIFICATION_QUEUED = ":LOAD_TMC_IDENTIFICATION_QUEUED",
  LOAD_TMC_IDENTIFICATION_DONE = ":LOAD_TMC_IDENTIFICATION_DONE",

  LOAD_NPMRDS_TRAVEL_TIMES_QUEUED = ":LOAD_NPMRDS_TRAVEL_TIMES_QUEUED",
  LOAD_NPMRDS_TRAVEL_TIMES_DONE = ":LOAD_NPMRDS_TRAVEL_TIMES_DONE",

  LINK_ETL_OUTPUT_INTO_DAMA_FILES_DONE = ":LINK_ETL_OUTPUT_INTO_DAMA_FILES_DONE",

  INTEGRATE_INTO_DAMA_VIEWS_DONE = ":INTEGRATE_INTO_DAMA_VIEWS_DONE",
}

const npmrds_travel_times_exports_data_dir = join(
  npmrds_data_dir,
  "npmrds_travel_times_export"
);

async function downloadAndTransformNpmrdsExport(
  npmrds_export_request: NpmrdsExportRequest
): Promise<NpmrdsExportFinalEvent> {
  const subtask_name = "download_and_transform_npmrds_export";

  const worker_path = join(
    __dirname,
    "../dt-npmrds_travel_times_export_ritis/worker.ts"
  );

  const initial_event: NpmrdsExportIntitialEvent = {
    type: ":INITIAL",
    payload: npmrds_export_request,
    meta: { note: "download and transform NPMRDS export" },
  };

  const dama_task_descriptor: QueuedDamaTaskDescriptor = {
    worker_path,
    dama_task_queue_name: NpmrdsTaskQueue.GENERAL_WORKER,
    parent_context_id: getEtlContextId(),
    initial_event,
    etl_work_dir: getEtlWorkDir(),
  };

  const subtask_config: SubtaskConfig = {
    subtask_name,
    dama_task_descriptor,
    subtask_queued_event_type: TaskEventType.DOWNLOAD_AND_TRANSFORM_QUEUED,
    subtask_done_event_type: TaskEventType.DOWNLOAD_AND_TRANSFORM_DONE,
  };

  return <NpmrdsExportFinalEvent>await doSubtask(subtask_config);
}

async function loadTmcIdentification(
  npmrds_export_transform_done_data: NpmrdsExportFinalEvent["payload"]
) {
  const subtask_name = "load_tmc_identifcation";

  const worker_path = join(
    __dirname,
    "../dt-npmrds_tmc_identification_imp/worker.ts"
  );

  const initial_event: LoadTmcIdentificationIntialEvent = {
    type: ":INITIAL",
    payload: npmrds_export_transform_done_data,
    meta: { note: "load TMC_Identification" },
  };

  const dama_task_descriptor: QueuedDamaTaskDescriptor = {
    worker_path,
    dama_task_queue_name: NpmrdsTaskQueue.GENERAL_WORKER,
    parent_context_id: getEtlContextId(),
    initial_event,
    etl_work_dir: getEtlWorkDir(),
  };

  const subtask_config: SubtaskConfig = {
    subtask_name,
    dama_task_descriptor,
    subtask_queued_event_type: TaskEventType.LOAD_TMC_IDENTIFICATION_QUEUED,
    subtask_done_event_type: TaskEventType.LOAD_TMC_IDENTIFICATION_DONE,
  };

  return <LoadTmcIdentificationFinalEvent>await doSubtask(subtask_config);
}

async function loadNpmrdsTravelTimes(
  npmrds_export_transform_done_data: NpmrdsExportFinalEvent["payload"],
  load_tmc_identifcation_done_data: LoadTmcIdentificationFinalEvent["payload"]
) {
  const subtask_name = "load_npmrds_travel_times";

  const worker_path = join(
    __dirname,
    "../dt-npmrds_travel_times_imp/worker.ts"
  );

  const initial_event: LoadNpmrdsTravelTimesInitialEvent = {
    type: ":INITIAL",
    payload: {
      npmrds_export_transform_done_data,
      load_tmc_identifcation_done_data,
    },
    meta: { note: "load NPMRDS travel times" },
  };

  const dama_task_descriptor: QueuedDamaTaskDescriptor = {
    worker_path,
    dama_task_queue_name: NpmrdsTaskQueue.GENERAL_WORKER,
    parent_context_id: getEtlContextId(),
    initial_event,
    etl_work_dir: getEtlWorkDir(),
  };

  const subtask_config: SubtaskConfig = {
    subtask_name,
    dama_task_descriptor,
    subtask_queued_event_type: TaskEventType.LOAD_NPMRDS_TRAVEL_TIMES_QUEUED,
    subtask_done_event_type: TaskEventType.LOAD_NPMRDS_TRAVEL_TIMES_DONE,
  };

  return <NpmrdsExportFinalEvent>await doSubtask(subtask_config);
}

async function linkEtlOutputIntoDamaFilesDir(
  download_and_transform_done_data: NpmrdsExportFinalEvent["payload"]
) {
  const events = await dama_events.getAllEtlContextEvents();

  let task_done_event = events.find(
    ({ type }) => type === TaskEventType.LINK_ETL_OUTPUT_INTO_DAMA_FILES_DONE
  );

  if (task_done_event) {
    return task_done_event.payload;
  }

  const { name: npmrdsDownloadName } = getNpmrdsExportMetadata();

  const new_export_data_dir = join(
    npmrds_travel_times_exports_data_dir,
    npmrdsDownloadName
  );

  const etl_work_dir = getEtlWorkDir();

  const new_export_root_relative_path = new_export_data_dir.replace(
    controllerDataDir,
    ""
  );

  const linkFileIntoDamaFilesDir = (tmp_etl_path: string) => {
    // The path of the file in the dama-files directory.
    const dama_files_path = tmp_etl_path.replace(
      etl_work_dir,
      new_export_data_dir
    );

    // If the file is nested in a subdirectory, create that subdirectory in the new_export_data_dir.
    mkdirSync(dirname(dama_files_path), { recursive: true });

    // The path of the file relative to the dama-files directory.
    const dama_relative_path =
      dama_files_path?.replace(
        new_export_data_dir,
        new_export_root_relative_path
      ) || null;

    // Create a hard link from the etl-dir file to the dama-files path.
    // Using hard links because they will
    //   * take no additional space (unlike copy) and
    //   * will preserve the etl-work-dir in case we need to debug/analyze.
    //
    // See https://nodejs.org/api/fs.html#fslinksyncexistingpath-newpath
    if (!existsSync(dama_files_path)) {
      linkSync(tmp_etl_path, dama_files_path);
    }

    return dama_relative_path;
  };

  const createDamaFileLinks = (elemNames: string[]) =>
    elemNames.reduce((acc, name) => {
      // Make the first character of the name lower case.
      const k = name.charAt(0).toLowerCase() + name.slice(1);

      const tmp_etl_path = download_and_transform_done_data[k] || null;

      if (!tmp_etl_path) {
        acc[name] = {
          host: dama_host_id,
          path: null,
        };

        return acc;
      }

      const dama_relative_path = linkFileIntoDamaFilesDir(tmp_etl_path);

      acc[name] = {
        host: dama_host_id,
        path: dama_relative_path,
      };

      return acc;
    }, {});

  const rawExportFiles = createDamaFileLinks([
    NpmrdsTravelTimesExportRitisElements.NpmrdsAllVehiclesTravelTimesExport,
    NpmrdsTravelTimesExportRitisElements.NpmrdsPassengerVehiclesTravelTimesExport,
    NpmrdsTravelTimesExportRitisElements.NpmrdsFreightTrucksTravelTimesExport,
  ]);

  const etlFiles = createDamaFileLinks([
    NpmrdsTravelTimesExportEtlElements.NpmrdsTravelTimesExportSqlite,
    NpmrdsTravelTimesExportEtlElements.NpmrdsTmcIdentificationCsv,
    NpmrdsTravelTimesExportEtlElements.NpmrdsTravelTimesCsv,
  ]);

  linkFileIntoDamaFilesDir(getNpmrdsExportMetadataFilePath());
  linkFileIntoDamaFilesDir(join(getEtlMetadataDir(), "PDA_APP_STORE.json"));

  const done_data = {
    [NpmrdsDataSources.NpmrdsTravelTimesExportRitis]: {
      statistics: getNpmrdsExportMetadata(),

      metadata: {
        files: rawExportFiles,
      },
    },
    [NpmrdsDataSources.NpmrdsTravelTimesExportEtl]: {
      metadata: {
        files: etlFiles,
      },
    },
  };

  task_done_event = {
    type: TaskEventType.LINK_ETL_OUTPUT_INTO_DAMA_FILES_DONE,
    payload: done_data,
  };

  await dama_events.dispatch(task_done_event);

  return done_data;
}

async function integrateNpmrdsTravelTimesEtlIntoDataManager(
  npmrds_export_metadata: NpmrdsExportMetadata,
  etl_done_data: EtlDoneData
): Promise<Array<DamaView>> {
  const events = await dama_events.getAllEtlContextEvents();

  let task_done_event = events.find(
    ({ type }) => type === TaskEventType.INTEGRATE_INTO_DAMA_VIEWS_DONE
  );

  if (task_done_event) {
    return task_done_event.payload;
  }

  const {
    name: npmrdsDownloadName,
    state,
    year,
    start_date,
    end_date,
  } = npmrds_export_metadata;

  const state_fips_code = stateAbbr2FipsCode[state];

  const [, download_timestamp] = npmrdsDownloadName.match(/_v([0-9t]+)$/i)!;

  // Inserts into data_manager.views with populated dependencies row.
  const sql = dedent(`
    WITH cte_deps AS (
      SELECT
          array_agg(view_id ORDER BY view_id) AS deps
        FROM data_manager.views
        WHERE (
          ( etl_context_id = $1 )         -- $1
          AND
          ( source_id = ANY( $2 ) )       -- $2
        )
    )
      INSERT INTO data_manager.views (
        etl_context_id,                   -- $1
        source_id,                        -- $3
        version,                          -- $4
        start_date,                       -- $5
        end_date,                         -- $6
        last_updated,                     -- $7
        interval_version,                 -- $8
        geography_version,                -- $9

                                          -- column values from etl_done_data
        table_schema,                     -- $10
        table_name,                       -- $11
        metadata,                         -- $12
        statistics,                       -- $13
        view_dependencies                 -- from cte_deps
      ) VALUES (
        $1,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9,
        $10,
        $11,
        $12,
        $13,
        ( SELECT deps FROM cte_deps )
      ) 
        RETURNING *
    ;
  `);

  const column_values_from_etl_done_data = [
    "table_schema",
    "table_name",
    "metadata",
    "statistics",
  ];

  const toposorted_src_info = await initializeDamaSources();

  const etl_context_id = getEtlContextId();

  const yearly_dama_source_names: Set<string> = new Set([
    NpmrdsTravelTimesExportEtlElements.NpmrdsTmcIdentificationCsv,
    NpmrdsDataSources.NpmrdsTmcIdentificationImports,
  ]);

  const view_info_by_src_id: Record<number, DamaView> = {};

  await dama_db.runInTransactionContext(async () => {
    for (const {
      name,
      source_id,
      source_dependencies,
    } of toposorted_src_info) {
      const dama_src_done_data = etl_done_data[name];

      logger.debug(
        `integrate into dama_views: dama_src_name=${name}, dama_src_done_data=${JSON.stringify(
          dama_src_done_data,
          null,
          4
        )}`
      );

      if (!dama_src_done_data) {
        continue;
      }

      const [_startDate, _endDate] = yearly_dama_source_names.has(name)
        ? [`${year}-01-01`, `${year}-12-31`]
        : [start_date, end_date];

      const start_date_numeric = _startDate.replace(/[^0-9]/g, "");
      const end_date_numeric = _endDate.replace(/[^0-9]/g, "");

      const intervalVersion = `${start_date_numeric}-${end_date_numeric}`;

      const values = [
        etl_context_id,
        source_dependencies,
        source_id,
        npmrdsDownloadName,
        _startDate,
        _endDate,
        download_timestamp,
        intervalVersion, // TODO : yrmo: Null if not complete month. Check for making authoritative.
        state_fips_code,
      ];

      column_values_from_etl_done_data.forEach((col) => {
        const v = dama_src_done_data[col];

        values.push(v === undefined ? null : v);
      });

      logger.silly(
        `integrate into dama_views: INSERT values=${JSON.stringify(
          values,
          null,
          4
        )}`
      );

      const {
        rows: [new_dama_view],
      } = await dama_db.query({ text: sql, values });

      view_info_by_src_id[source_id] = new_dama_view;
    }
  });

  const done_data = toposorted_src_info
    .map((d: any) => {
      const viewInfo = view_info_by_src_id[d.source_id];

      if (!viewInfo) {
        return null;
      }

      return { ...d, ...viewInfo };
    })
    .filter(Boolean);

  task_done_event = {
    type: TaskEventType.INTEGRATE_INTO_DAMA_VIEWS_DONE,
    payload: done_data,
  };

  await dama_events.dispatch(task_done_event);

  return done_data;
}

export default async function main(
  initial_event: InitialEvent
): Promise<DoneData> {
  verifyIsInTaskEtlContext();

  this.logger.info(`==> aggregate-etl.main pid=${process.pid}`);

  const events = await dama_events.getAllEtlContextEvents();

  let final_event = events.find(({ type }) => type === ":FINAL");

  if (final_event) {
    return final_event.payload;
  }

  const { payload: npmrds_export_request } = initial_event;

  const { payload: download_and_transform_done_data } =
    await downloadAndTransformNpmrdsExport(npmrds_export_request);

  const { payload: load_tmc_identifcation_done_data } =
    await loadTmcIdentification(download_and_transform_done_data);

  const { payload: load_npmrds_travel_times_done_data } =
    await loadNpmrdsTravelTimes(
      download_and_transform_done_data,
      load_tmc_identifcation_done_data
    );

  const etl_output_into_dama_files_done_data =
    await linkEtlOutputIntoDamaFilesDir(download_and_transform_done_data);

  const etl_done_data = {
    ...etl_output_into_dama_files_done_data,

    [NpmrdsDataSources.NpmrdsTmcIdentificationImports]:
      load_tmc_identifcation_done_data,

    [NpmrdsDataSources.NpmrdsTravelTimesImports]:
      load_npmrds_travel_times_done_data,
  };

  const npmrds_export_metadata = getNpmrdsExportMetadata();

  const integrate_done_data =
    await integrateNpmrdsTravelTimesEtlIntoDataManager(
      npmrds_export_metadata,
      etl_done_data
    );

  const done_data = {
    etl_context_id: getEtlContextId()!,
    npmrds_export_metadata,
    etl_done_data,
    integrate_done_data,
  };

  final_event = {
    type: ":FINAL",
    payload: done_data,
  };

  await dama_events.dispatch(final_event);

  return done_data;
}
