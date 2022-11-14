import _ from "lodash";

import { Context } from "moleculer";
import { FSA } from "flux-standard-action";

import EtlDamaCreateEventTypes from "../../../../dama_meta/constants/EventTypes";

import GisDatasetIntegrationEventTypes from "../../../constants/EventTypes";

export const ReadyToPublishPrerequisites = [
  GisDatasetIntegrationEventTypes.QA_APPROVED,
  EtlDamaCreateEventTypes.QUEUE_CREATE_NEW_DAMA_VIEW,
];

export default async function checkIfReadyToPublish(
  ctx: Context,
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

    await ctx.call("dama_dispatcher.dispatch", errEvent);

    console.error("errEvent:", errEvent);

    throw new Error(message);
  }
}
