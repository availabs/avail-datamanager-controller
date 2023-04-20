import _ from "lodash";

import { Context as MoleculerContext } from "moleculer";

import { FSA } from "flux-standard-action";

import EventTypes from "../../../constants/EventTypes";

import GisDatasetIntegrationEventTypes from "../../../constants/EventTypes";

export const ReadyToPublishPrerequisites = [
  EventTypes.QA_APPROVED,
  EventTypes.QUEUE_CREATE_NEW_DAMA_VIEW,
];

export default async function checkIfReadyToPublish(
  ctx: MoleculerContext,
  events: FSA[]
) {
  const {
    // @ts-ignore
    params: { etl_context_id },
  } = ctx;

  const eventTypes = events.map(({ type }) => type);

  const unmetPreReqs = _.difference(
    ReadyToPublishPrerequisites,
    eventTypes
  ).map((e) => e.replace(/.*:/, ""));

  if (unmetPreReqs.length) {
    const message = `The following PUBLISH prerequisites are not met: ${unmetPreReqs}`;

    const errEvent = {
      type: GisDatasetIntegrationEventTypes.NOT_READY_TO_PUBLISH,
      payload: {
        message,
      },
      meta: {
        etl_context_id,
        timestamp: new Date().toISOString(),
      },
      error: true,
    };

    await ctx.call("data_manager/events.dispatch", errEvent);

    console.error("errEvent:", errEvent);

    throw new Error(message);
  }
}
