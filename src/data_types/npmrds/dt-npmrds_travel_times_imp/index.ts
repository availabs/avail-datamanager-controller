import dama_events from "data_manager/events";

import { FinalEvent as NpmrdsExportFinalEvent } from "../dt-npmrds_travel_times_export_ritis";
import { FinalEvent as LoadTmcIdentificationFinalEvent } from "../dt-npmrds_tmc_identification_imp";

import load, { DoneData as LoadDoneData } from "./tasks/load";

import compute_statistics, {
  DoneData as StatisticsDoneData,
} from "./tasks/compute_statistics";

export type DoneData = LoadDoneData & { statistics: StatisticsDoneData };

export type InitialEvent = {
  type: ":INITIAL";
  payload: {
    npmrds_export_transform_done_data: NpmrdsExportFinalEvent["payload"];
    load_tmc_identifcation_done_data: LoadTmcIdentificationFinalEvent["payload"];
  };
};

export type FinalEvent = {
  type: ":FINAL";
  payload: DoneData;
};

enum StepDoneType {
  LOAD_DONE = "LOAD_DONE",
  COMPUTE_STATS_DONE = "COMPUTE_STATS_DONE",
}

export default async function main(
  initial_event: InitialEvent
): Promise<DoneData> {
  const {
    payload: {
      npmrds_export_transform_done_data: { npmrdsTravelTimesSqliteDb },
      load_tmc_identifcation_done_data: {
        table_schema: tmc_identification_imp_table_schema,
        table_name: tmc_identification_imp_table_name,
      },
    },
  } = initial_event;

  const events = await dama_events.getAllEtlContextEvents();

  let final_event = events.find(({ type }) => type === ":FINAL");

  if (final_event) {
    return final_event.payload;
  }

  let load_done_event = events.find(
    ({ type }) => type === StepDoneType.LOAD_DONE
  );

  if (!load_done_event) {
    const load_done_data = await load(npmrdsTravelTimesSqliteDb);

    load_done_event = {
      type: StepDoneType.LOAD_DONE,
      payload: load_done_data,
    };

    await dama_events.dispatch(load_done_event);
  }

  const {
    payload: { table_name, table_schema, metadata },
  } = load_done_event;

  let compute_stats_done_event = events.find(
    ({ type }) => type === StepDoneType.COMPUTE_STATS_DONE
  );

  if (!compute_stats_done_event) {
    const compute_stats_done_data = await compute_statistics(
      table_schema,
      table_name,
      tmc_identification_imp_table_schema,
      tmc_identification_imp_table_name
    );

    compute_stats_done_event = {
      type: StepDoneType.COMPUTE_STATS_DONE,
      payload: compute_stats_done_data,
    };

    await dama_events.dispatch(compute_stats_done_event);
  }

  const { payload: statistics } = compute_stats_done_event;

  const done_data = { table_schema, table_name, metadata, statistics };

  final_event = {
    type: ":FINAL",
    payload: done_data,
  };

  await dama_events.dispatch(final_event);

  return done_data;
}
