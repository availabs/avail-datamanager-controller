import _ from "lodash";

import EtlDamaCreateEventTypes from "../../../../dama_meta/constants/EventTypes";

import { TransactionContext, DamaSource } from "../index.d";

export default async function createNewDamaSource(txnCtx: TransactionContext) {
  const { params } = txnCtx;

  const { eventsByType } = params;

  // There may or may not be a createDamaSourceEvent
  const createDamaSourceEvent = _.get(eventsByType, [
    EtlDamaCreateEventTypes.QUEUE_CREATE_NEW_DAMA_SOURCE,
    0,
  ]);

  if (createDamaSourceEvent) {
    const newDamaSource = <DamaSource>(
      await txnCtx.call(
        "dama/metadata.createNewDamaSource",
        createDamaSourceEvent.payload
      )
    );

    params.newDamaSource = newDamaSource;
  }
}
