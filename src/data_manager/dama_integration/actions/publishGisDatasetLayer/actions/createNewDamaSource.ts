import _ from "lodash";

import { Context as MoleculerContext } from "moleculer";

import EventTypes from "../../../constants/EventTypes";

import { DamaSource } from "../index.d";

export default async function createNewDamaSource(ctx: MoleculerContext) {
  const {
    params,
    meta: { etl_context_id },
  } = ctx;

  const { eventsByType } = params;

  // There may or may not be a createDamaSourceEvent
  const createDamaSourceEvent = _.get(eventsByType, [
    EventTypes.QUEUE_CREATE_NEW_DAMA_SOURCE,
    0,
  ]);

  if (createDamaSourceEvent) {
    const newDamaSource = <DamaSource>(
      await ctx.call(
        "dama/metadata.createNewDamaSource",
        createDamaSourceEvent.payload
      )
    );

    const { source_id } = newDamaSource;

    await ctx.call("data_manager/events.setEtlContextSourceId", {
      etl_context_id,
      source_id,
    });

    params.newDamaSource = newDamaSource;
  }

  return null;
}
