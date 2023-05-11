import { join } from "path";

import _ from "lodash";
import { DateTime } from "luxon";

import dama_db from "data_manager/dama_db";
import dama_meta from "data_manager/meta";
import dama_events from "data_manager/events";
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
} from "./domain";

import { InitialEvent as NpmrdsEtlInitialEvent } from ".";

import { partitionDateRange } from "./utils/dates";

import MassiveDataDownloader from "./dt-npmrds_travel_times_export_ritis/puppeteer/MassiveDataDownloader";

export type InitialEvent = {
  type: ":INITIAL";
  payload?: { states: NpmrdsState[] } | null;
};

export enum EventType {
  "GOT_PARITIONS" = "GOT_PARITIONS",
}

const npmrds_etl_worker_path = join(__dirname, "./worker.ts");

// So we don't inadvertently request years of data from RITIS.
const MAX_DOWNLOAD_PARTITIONS_PER_STATE = 3;

export default async function main(initial_event: InitialEvent) {
  verifyIsInTaskEtlContext();

  const { states = [NpmrdsState.ny, NpmrdsState.nj] } =
    initial_event.payload || {};

  const events = await dama_events.getAllEtlContextEvents();

  let final_event = events.find(({ type }) => type === ":FINAL");

  if (final_event) {
    return final_event.payload;
  }

  let got_paritions_event = events.find(
    ({ type }) => type === EventType.GOT_PARITIONS
  );

  if (!got_paritions_event) {
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

    const partitions = _.flatten(
      await Promise.all(
        states.map(async (state) => {
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

          if (next_date < max_data_date) {
            const foo = partitionDateRange(next_date, max_data_date);

            if (foo.length > MAX_DOWNLOAD_PARTITIONS_PER_STATE) {
              throw new Error(
                `Too large a gap for ${state}: ${next_date} to ${max_data_date}`
              );
            }

            return foo.map(({ start_date, end_date }) => {
              const start = start_date.replace(/-/g, "");
              const end = end_date.replace(/-/g, "");

              return {
                subtask_name: `${state}/${start}-${end}`,
                export_request: {
                  state,
                  start_date,
                  end_date,
                  // is_expanded: true,
                  is_expanded: false,
                },
              };
            });
          }

          return null;
        })
      )
    ).filter(Boolean);

    got_paritions_event = {
      type: EventType.GOT_PARITIONS,
      payload: partitions,
    };

    await dama_events.dispatch(got_paritions_event);
  }

  console.log(JSON.stringify({ got_paritions_event }, null, 4));

  const done_data = await Promise.all(
    got_paritions_event.payload.map(
      async ({ subtask_name, export_request }) => {
        const task_initial_event: NpmrdsEtlInitialEvent = {
          type: ":INITIAL",
          payload: export_request,
        };

        const dama_task_descriptor: QueuedDamaTaskDescriptor = {
          worker_path: npmrds_etl_worker_path,
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
