import dama_events from "data_manager/events";
import logger from "data_manager/logger";
import { verifyIsInTaskEtlContext } from "data_manager/contexts";

import extract_and_transform_npmrds_export from "./steps/extract_and_transform_npmrds_export";

import { NpmrdsExportTransformOutput } from "data_types/npmrds/dt-npmrds_travel_times_export_ritis/domain";

export type InitialEvent = {
  type: ":INITIAL";
};

export type FinalEvent = {
  type: ":FINAL";
  payload: NpmrdsExportTransformOutput;
};

export default async function main(): Promise<NpmrdsExportTransformOutput> {
  verifyIsInTaskEtlContext();

  const events = await dama_events.getAllEtlContextEvents();

  let final_event: FinalEvent | undefined = events.find(
    ({ type }) => type === ":FINAL"
  );

  if (final_event) {
    return final_event.payload;
  }

  try {
    const done_data = await extract_and_transform_npmrds_export();

    final_event = {
      type: ":FINAL",
      payload: done_data,
    };

    await dama_events.dispatch(final_event);

    return done_data;
  } catch (err) {
    const { message, stack } = <Error>err;

    logger.error(message);
    logger.error(stack);

    await dama_events.dispatch({
      type: ":ERROR",
      payload: { message, stack },
      error: true,
    });

    throw err;
  }
}
