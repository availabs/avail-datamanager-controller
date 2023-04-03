// select * from data_manager.etl_contexts where etl_context_id = 900 or parent_context_id = 900 order by 1;

import { join } from "path";

import dedent from "dedent";
import { FSA } from "flux-standard-action";

import dama_db from "../../../dama_db";
import dama_events from "../../../events";

import { getEtlContextId } from "../../../contexts";
import { getLoggerForContext, LoggingLevel } from "../../../logger";

import BaseTasksController from "../../BaseTasksController";

import { injectChaos } from "./chaos";

const task_controller = new BaseTasksController();

const dama_task_queue_name =
  "data_manager/dama_tasks/examples/chaotic_concurrent_subtasks_fizzbuzz";

const logger = getLoggerForContext();
logger.level = process.env.AVAIL_LOGGING_LEVEL || LoggingLevel.debug;

const worker_path = join(__dirname, "./subtask_worker.ts");

export default async function main(initial_event: FSA): Promise<FSA> {
  logger.debug("subtasks_workflow worker main start");

  const heartbeat_interval = setInterval(
    () => logger.debug(`HEARTBEAT: ${new Date().toISOString()}`),
    2000
  );

  await dama_events.dispatch({ type: ":TASK_STARTED" });

  let {
    // @ts-ignore
    // eslint-disable-next-line prefer-const
    payload: { n, iterations, chaos_factor = 0.05 },
  } = initial_event;

  injectChaos(chaos_factor);

  const parent_context_id = getEtlContextId();

  const subtask_promises: Array<Promise<FSA>> = [];

  while (iterations--) {
    const dama_task_descr = {
      worker_path,
      dama_task_queue_name,
      parent_context_id,
      initial_event: {
        type: ":INITIAL",
        payload: { n, chaos_factor: Math.random() * 0.333 },
        meta: { idempotency_key: `subtask_${n}` },
      },
    };

    subtask_promises.push(
      new Promise(async (resolve) => {
        const _n = n;

        injectChaos(chaos_factor);

        const idempotency_sql = dedent(`
          SELECT
              etl_context_id
            FROM data_manager.etl_contexts AS a
              INNER JOIN data_manager.event_store AS c
                USING ( etl_context_id )
            WHERE (
              ( a.initial_event_id = c.event_id )
              AND
              ( a.parent_context_id = $1 )
              AND
              ( c.meta->>'idempotency_key' = $2 )
            )
        `);

        let {
          rows: [{ etl_context_id = null } = {}],
        } = await dama_db.query({
          text: idempotency_sql,
          values: [parent_context_id, `subtask_${_n}`],
        });

        logger.debug(
          `idempotency check: subtask_${_n} parent_context_id=${parent_context_id} etl_context_id=${etl_context_id}`
        );

        if (!etl_context_id) {
          injectChaos(chaos_factor);

          logger.debug(`before queueing subtask ${_n} `);

          try {
            // @ts-ignore
            etl_context_id = (
              await task_controller.queueDamaTask(dama_task_descr, {
                retryLimit: 1e6,
                expireInHours: 24,
              })
            ).etl_context_id;
          } catch (err) {
            console.error(err);
            // @ts-ignore
            logger.error(err.message);
          }

          logger.debug(`before queueing subtask ${_n} `);

          logger.debug(`queued subtask ${_n} etl_context_id=${etl_context_id}`);

          injectChaos(chaos_factor);
        } else {
          logger.debug(
            `subtask ${_n} etl_context_id=${etl_context_id} already queued`
          );
        }

        dama_events.registerEtlContextFinalEventListener(
          etl_context_id,
          resolve
        );

        logger.debug(`registered :FINAL event listener for subtask ${_n}`);
      })
    );

    injectChaos(chaos_factor);

    ++n;
  }

  injectChaos(chaos_factor);

  await Promise.all(subtask_promises);

  injectChaos(chaos_factor);

  const summary_sql = dedent(`
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

  injectChaos(chaos_factor);

  const { rows: summary } = await dama_db.query({
    text: summary_sql,
    values: [parent_context_id],
  });

  injectChaos(chaos_factor);

  clearInterval(heartbeat_interval);

  return {
    type: ":FINAL",
    // @ts-ignore
    payload: { summary },
  };
}
