import { inspect } from "util";

import { FSA } from "flux-standard-action";
import dama_events from "../../../events";

import { getLoggerForContext, LoggingLevel } from "../../../logger";

const logger = getLoggerForContext();
logger.level = LoggingLevel.debug;

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

export default async function main(initial_event: FSA): Promise<FSA> {
  logger.info(`tasks/examples/simple_foo_bar/worker.ts ${new Date()}: start`);

  logger.debug(inspect(initial_event));

  const {
    // @ts-ignore
    payload: { msg, delay },
  } = initial_event;

  injectChaos();

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
    // @ts-ignore
    payload: { msg },
  };
}
