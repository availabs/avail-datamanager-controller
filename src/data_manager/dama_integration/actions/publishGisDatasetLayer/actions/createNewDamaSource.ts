import _ from "lodash";

import EventTypes from "../../../constants/EventTypes";

import { TransactionContext, DamaSource } from "../index.d";

export default async function createNewDamaSource(txnCtx: TransactionContext) {
  const {
    params,
    meta: { etl_context_id },
  } = txnCtx;

  const { eventsByType } = params;

  // There may or may not be a createDamaSourceEvent
  const createDamaSourceEvent = _.get(eventsByType, [
    EventTypes.QUEUE_CREATE_NEW_DAMA_SOURCE,
    0,
  ]);

  if (createDamaSourceEvent) {
    const newDamaSource = <DamaSource>(
      await txnCtx.call(
        "dama/metadata.createNewDamaSource",
        createDamaSourceEvent.payload
      )
    );

    const { source_id } = newDamaSource;

    await txnCtx.call("data_manager/events.setEtlContextSourceId", {
      etl_context_id,
      source_id,
    });

    params.newDamaSource = newDamaSource;
  }

  return null;
}
