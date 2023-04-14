import { inspect } from "util";

import dedent from "dedent";
import _ from "lodash";

import dama_db from "data_manager/dama_db";
import dama_events from "data_manager/events";
import { TaskEtlContext } from "data_manager/contexts";
import { LoggingLevel } from "data_manager/logger";

import { injectChaos } from "./chaos";

export type InitialEvent = {
  type: ":INITIAL";
  payload: {
    n: number;
    iterations: number;
    chaos_factor: number;
  };
};

export type FinalEvent = {
  type: ":FINAL";
  payload: { n: number };
};

// NOTE: Does not run in a TaskEtlContext
export default async function main(
  etl_context: TaskEtlContext
): Promise<FinalEvent> {
  const {
    initial_event,
    logger,
    meta: { etl_context_id, pgEnv: pg_env },
  } = etl_context;

  logger.level = process.env.AVAIL_LOGGING_LEVEL || LoggingLevel.debug;

  logger.debug(`TaskEtlContext ${inspect(_.omit(etl_context, "logger"))}`);

  const {
    payload: { n, chaos_factor },
  } = initial_event;

  const heartbeat_interval = setInterval(
    () => logger.debug(`HEARTBEAT: ${new Date().toISOString()}`),
    2000
  );

  await dama_events.dispatch({ type: ":TASK_STARTED" }, etl_context_id, pg_env);

  injectChaos(chaos_factor, logger);

  let type = n % 3 === 0 ? "FIZZ" : "";

  if (n % 5 === 0) {
    type = `${type}BUZZ`;
  }

  if (type) {
    type = `:${type}`;

    const idempotency_sql = dedent(`
      SELECT NOT EXISTS (
        SELECT
            1
          FROM data_manager.etl_contexts AS a
            INNER JOIN data_manager.event_store AS b
              USING ( etl_context_id )
          WHERE (
            ( etl_context_id = $1 )
            AND
            ( type = $2 )
          )
      ) AS not_yet_dispatched
    `);

    const {
      rows: [{ not_yet_dispatched }],
    } = await dama_db.query(
      {
        text: idempotency_sql,
        values: [etl_context_id, type],
      },
      pg_env
    );

    injectChaos(chaos_factor, logger);

    if (not_yet_dispatched) {
      dama_events.dispatch(
        {
          type,
        },
        etl_context_id,
        pg_env
      );
    }

    injectChaos(chaos_factor, logger);
  }

  injectChaos(chaos_factor, logger);

  clearInterval(heartbeat_interval);

  return {
    type: ":FINAL",
    payload: { n: n + 1 },
  };
}
