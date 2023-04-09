import { inspect } from "util";

import dama_events from "data_manager/events";
import { TaskEtlContext } from "data_manager/contexts";
import { Logger } from "data_manager/logger";

const CHAOS_FACTOR = 0;
// const CHAOS_FACTOR = 0.1;
// const CHAOS_FACTOR = 1;

function injectChaos(logger: Logger) {
  if (Math.random() < CHAOS_FACTOR) {
    if (Math.random() < 0.5) {
      logger.error("Chaos kill.");
      process.exit(111);
    }

    throw new Error("Wildcard!!!");
  }
}

async function doTask(
  type: string,
  event_types: Set<string>,
  chaos: Function,
  etl_context_id: number,
  pg_env: string
) {
  chaos();

  if (event_types.has(type)) {
    return;
  }

  await dama_events.dispatch({ type }, etl_context_id, pg_env);
}

const foo = doTask.bind(null, ":FOO");
const bar = doTask.bind(null, ":BAR");
const baz = doTask.bind(null, ":BAZ");

const workflow = [foo, bar, baz];

export type InitialEvent = {
  type: ":INITIAL";
  payload: { msg: string; delay: number };
};

export type FinalEvent = {
  type: ":FINAL";
  payload: { msg: string };
};

// NOTE: Does NOT run in a TaskEtlContext
export default async function main(
  etl_context: TaskEtlContext
): Promise<FinalEvent> {
  const {
    initial_event,
    logger,
    meta: { pgEnv: pg_env, etl_context_id },
  } = etl_context;

  const chaos = injectChaos.bind(null, logger);

  logger.info(
    `tasks/examples/simple_foo_bar/worker.without-context.ts ${new Date()}: start`
  );

  logger.debug(inspect(initial_event));

  const {
    payload: { msg, delay },
  } = initial_event;

  chaos();

  //  This worker gets pg_env and etl_context_id from DamaEtlContext.
  //    Could alternatively get them from initial_event.
  const events = await dama_events.getAllEtlContextEvents(
    etl_context_id,
    pg_env
  );

  chaos();

  const event_types = new Set(events.map(({ type }) => type));

  chaos();

  for (const task of workflow) {
    chaos();

    await task(event_types, chaos, etl_context_id, pg_env);

    await new Promise((resolve) => setTimeout(resolve, delay));
  }

  chaos();

  return {
    type: ":FINAL",
    payload: { msg },
  };
}
