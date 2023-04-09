import { join } from "path";

import dedent from "dedent";

import dama_db from "../../../dama_db";
import dama_events, { DamaEvent, EtlEvent } from "../../../events";

import { getEtlContextId } from "../../../contexts";
import { getLoggerForContext, LoggingLevel } from "../../../logger";

import BaseTasksController from "../../BaseTasksController";
const task_controller = new BaseTasksController();

const dama_task_queue_name = "data_manager/dama_tasks/examples/fizzbuzz";

const ITERATIONS = 30;

const logger = getLoggerForContext();
logger.level = LoggingLevel.debug;

const worker_path = join(__dirname, "./subtask_worker.ts");

export default async function main(initial_event: EtlEvent): Promise<EtlEvent> {
  logger.debug("subtasks_workflow worker main start");

  const parent_context_id = getEtlContextId();

  let { n } = initial_event.payload;

  for (let i = 0; i < ITERATIONS; ++i) {
    const dama_task_descr = {
      worker_path,
      dama_task_queue_name,
      parent_context_id,
      initial_event: {
        type: ":INITIAL",
        payload: { n },
      },
    };

    // @ts-ignore
    const { etl_context_id } = await task_controller.queueDamaTask(
      dama_task_descr
    );

    const final_event = <DamaEvent>(
      await new Promise((resolve) =>
        dama_events.registerEtlContextFinalEventListener(
          etl_context_id,
          resolve
        )
      )
    );

    logger.debug(`Subtask :FINAL event ${JSON.stringify(final_event)}`);

    n = final_event.payload.n;
  }

  const text = dedent(`
    SELECT
        b.type,
        COUNT(1) AS type_count
      FROM data_manager.etl_contexts AS a
        INNER JOIN data_manager.event_store AS b
          USING ( etl_context_id )
      WHERE (
        ( a.parent_context_id = $1 )
        AND
        ( b.type = ANY( ARRAY[ ':FIZZ', ':BUZZ', ':FIZZBUZZ' ] ) )
      )
      GROUP BY 1
  `);

  const { rows: summary } = await dama_db.query({
    text,
    values: [parent_context_id],
  });

  return {
    type: ":FINAL",
    payload: { summary },
  };
}
