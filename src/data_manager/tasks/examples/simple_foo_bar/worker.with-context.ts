import { inspect } from "util";

import dama_events from "data_manager/events";

import logger from "data_manager/logger";

import {
  runInDamaContext,
  isInTaskEtlContext,
  TaskEtlContext,
} from "data_manager/contexts";

const CHAOS_FACTOR = 0;
// const CHAOS_FACTOR = 0.1;
// const CHAOS_FACTOR = 1;

function injectChaos() {
  if (Math.random() < CHAOS_FACTOR) {
    if (Math.random() < 0.5) {
      logger.error("Chaos kill.");
      process.exit(111);
    }

    throw new Error("Wildcard!!!");
  }
}

async function doTask(type: string, event_types: Set<string>) {
  injectChaos();

  if (event_types.has(type)) {
    return;
  }

  await dama_events.dispatch({ type });
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

export async function main(initial_event: InitialEvent): Promise<FinalEvent> {
  if (!isInTaskEtlContext()) {
    throw new Error("MUST run in a TaskEtlContext");
  }

  logger.info(
    `tasks/examples/simple_foo_bar/worker.with-context.ts ${new Date()}: start`
  );

  logger.debug(inspect(initial_event));

  const {
    payload: { msg, delay },
  } = initial_event;

  injectChaos();

  //  This worker gets pg_env and etl_context_id from DamaEtlContext.
  //    Could alternatively get them from initial_event.
  const events = await dama_events.getAllEtlContextEvents();

  injectChaos();

  const event_types = new Set(events.map(({ type }) => type));

  injectChaos();

  for (const task of workflow) {
    injectChaos();

    await task(event_types);

    await new Promise((resolve) => setTimeout(resolve, delay));
  }

  injectChaos();

  return {
    type: ":FINAL",
    payload: { msg },
  };
}

export default (etl_context: TaskEtlContext) =>
  runInDamaContext(etl_context, () =>
    main(<InitialEvent>etl_context.initial_event)
  );
