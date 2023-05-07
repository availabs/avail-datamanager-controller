import dama_events from "data_manager/events";
import { verifyIsInTaskEtlContext } from "data_manager/contexts";

import { NpmrdsExportTransformOutput } from "data_types/npmrds/domain";

import load, { DoneData as LoadDoneData } from "./tasks/load";
import compute_statistics, {
  DoneData as StatisticsDoneData,
} from "./tasks/compute_statistics";

export type DoneData = LoadDoneData & { statistics: StatisticsDoneData };

export type InitialEvent = {
  type: ":INITIAL";
  payload: {
    npmrdsTravelTimesSqliteDb: NpmrdsExportTransformOutput["npmrdsTravelTimesSqliteDb"];
  };
};

export type FinalEvent = {
  type: ":FINAL";
  payload: DoneData;
};

export default async function main(
  initial_event: InitialEvent
): Promise<DoneData> {
  verifyIsInTaskEtlContext();

  const {
    payload: { npmrdsTravelTimesSqliteDb },
  } = initial_event;

  const events = await dama_events.getAllEtlContextEvents();

  let final_event = events.find(({ type }) => type === ":FINAL");

  if (final_event) {
    return final_event.payload;
  }

  const load_done_data = await load(npmrdsTravelTimesSqliteDb);

  const statistics = await compute_statistics(load_done_data);

  const done_data = { ...load_done_data, statistics };

  final_event = {
    type: ":FINAL",
    payload: done_data,
  };

  await dama_events.dispatch(final_event);

  return done_data;
}
