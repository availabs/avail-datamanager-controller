import dama_db from "data_manager/dama_db";
import dama_events from "data_manager/events";
import logger from "data_manager/logger";
import { verifyIsInTaskEtlContext } from "data_manager/contexts";

import publish_staged_events from "./steps/publish_staged_events";
import compute_transcom_events_admin_geographies from "./steps/compute_transcom_events_admin_geographies";
import snap_events_to_conflation_map from "./steps/snap_events_to_conflation_map";

export type InitialEvent = {
  type: ":INITIAL";
  payload: {
    etl_work_dir: string;
  };
};

export type FinalEvent = {
  type: ":FINAL";
};

export default async function main(etl_work_dir: string) {
  verifyIsInTaskEtlContext();

  const events = await dama_events.getAllEtlContextEvents();

  let final_event = events.find(({ type }) => type === ":FINAL");

  if (final_event) {
    logger.info("postprocessing already DONE");

    return final_event;
  }

  try {
    await dama_db.runInTransactionContext(async () => {
      logger.debug("calling publish_staged_events");

      await publish_staged_events(etl_work_dir);

      logger.debug("calling compute_transcom_events_admin_geographies");

      await compute_transcom_events_admin_geographies(etl_work_dir);

      logger.debug("calling snap_events_to_conflation_map");

      await snap_events_to_conflation_map();

      logger.debug("postprocessing DONE");
    });

    final_event = { type: ":FINAL" };

    await dama_events.dispatch(final_event);

    return final_event;
  } catch (err) {
    const err_event = {
      type: ":ERROR",
      payload: {
        message: (<Error>err).message,
        stack: (<Error>err).stack,
      },
      error: true,
    };

    await dama_events.dispatch(err_event);

    throw err;
  }
}
