import _ from "lodash";

import { Context } from "moleculer";
import { FSA } from "flux-standard-action";

import { DamaDbTransaction } from "../../../../dama_db/index.service";

import { TransactionContext } from "../index.d";

import checkIfReadyToPublish from "./checkIfReadyToPublish";

export default async function createTransactionContext(
  ctx: Context
): Promise<{ txn: DamaDbTransaction; txnCtx: TransactionContext }> {
  // throw new Error("publish TEST ERROR");
  const {
    // @ts-ignore
    params: { etl_context_id, user_id },
  } = ctx;

  if (!(etl_context_id && user_id)) {
    throw new Error("The etl_context_id and user_id parameters are required.");
  }

  const events: FSA[] = await ctx.call("data_manager/events.queryEvents", {
    etl_context_id,
  });

  await checkIfReadyToPublish(ctx, events);

  const eventsByType = events.reduce((acc, damaEvent: FSA) => {
    const { type } = damaEvent;

    acc[type] = acc[type] || [];
    acc[type].push(damaEvent);

    return acc;
  }, {});

  const txn: DamaDbTransaction = await ctx.call("dama_db.createTransaction");

  // @ts-ignore
  const txnCtx: TransactionContext = ctx.copy();
  txnCtx.params = {
    events,
    eventsByType,
    newDamaSource: null,
    newDamaView: null,
  };
  txnCtx.meta = {
    ..._.cloneDeep(ctx.meta),
    etl_context_id,
    transactionId: txn.transactionId,
  };

  return { txn, txnCtx };
}
