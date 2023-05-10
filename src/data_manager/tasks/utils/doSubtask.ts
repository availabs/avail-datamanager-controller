import { inspect } from "util";

import dama_events, { DamaEvent } from "data_manager/events";
import logger from "data_manager/logger";

import { DamaTaskDescriptor } from "../domain";

import BaseTasksController from "../BaseTasksController";

const tasks_controller = new BaseTasksController();

export type SubtaskConfig = {
  subtask_name: string;
  dama_task_descriptor: DamaTaskDescriptor;
  subtask_queued_event_type: string;
  subtask_done_event_type: string;
};

export type SubtaskFinalEvent = {
  type: string;
  meta: { etl_context_id: number };
};

export default async function doSubtask(
  config: SubtaskConfig
): Promise<DamaEvent> {
  const {
    subtask_name,
    dama_task_descriptor,
    subtask_queued_event_type,
    subtask_done_event_type,
  } = config;

  const events = await dama_events.getAllEtlContextEvents();

  logger.debug(`${subtask_name} events: ${JSON.stringify(events, null, 4)}`);

  const done_event = events.find((e) => e.type === subtask_done_event_type);

  // If the done_event already has been emitted for this subtask, return the subtask's final event.
  if (done_event) {
    logger.debug(`${subtask_name} already DONE`);

    const {
      payload: { etl_context_id },
    } = done_event;

    const subtask_final_event = await dama_events.getEtlContextFinalEvent(
      etl_context_id
    );

    return subtask_final_event;
  }

  let queued_event = events.find((e) => e.type === subtask_queued_event_type);

  //  If the queued_event has not already has been emitted for this subtask,
  //    we must queue the subtask.
  if (!queued_event) {
    logger.debug(`${subtask_name} must queue subtask`);

    // @ts-ignore FIXME
    const { etl_context_id: eci } = await tasks_controller.queueDamaTask(
      dama_task_descriptor
    );

    logger.debug(`${subtask_name} queued subtask etl_context_id=${eci}`);

    queued_event = {
      type: subtask_queued_event_type,
      payload: {
        subtask_name,
        etl_context_id: eci,
      },
    };

    logger.debug(
      `${subtask_name} dispatching queued_event ${inspect(queued_event)}`
    );

    await dama_events.dispatch(queued_event);
  }

  const {
    payload: { etl_context_id: subtask_eci },
  } = queued_event;

  logger.debug(`${subtask_name} awaiting subtask :FINAL event`);

  const final_event = await dama_events.getEventualEtlContextFinalEvent(
    subtask_eci
  );

  logger.debug(`${subtask_name} received subtask :FINAL event`);

  await dama_events.dispatch({
    type: subtask_done_event_type,
    payload: { etl_context_id: subtask_eci },
  });

  return final_event;
}
