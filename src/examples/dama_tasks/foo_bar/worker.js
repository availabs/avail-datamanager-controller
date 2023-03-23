#!/usr/bin/env node

require("ts-node").register({ logError: true });

import TaskManager from "../../../data_manager/tasks/TaskManager";

const CHAOS_FACTOR = 0;
// const CHAOS_FACTOR = 0.1;
// const CHAOS_FACTOR = 1;

function injectChaos() {
  if (Math.random() < CHAOS_FACTOR) {
    if (Math.random() < 0.5) {
      console.error("Chaos kill.");
      process.exit(111);
    }

    throw new Error("Wildcard!!!");
  }
}

async function doTask(type, event_types) {
  injectChaos();

  if (event_types.has(type)) {
    return;
  }

  await TaskManager.dispatchEvent({ type });
}

const foo = doTask.bind(null, ":FOO");
const bar = doTask.bind(null, ":BAR");
const baz = doTask.bind(null, ":BAZ");

const workflow = [foo, bar, baz];

async function main() {
  try {
    await TaskManager.ready;

    TaskManager.log(`${new Date()}: start`);

    injectChaos();

    const {
      payload: { msg, delay },
    } = TaskManager.getInitialEvent();

    injectChaos();

    const events = await TaskManager.getEtlContextEvents();

    const event_types = new Set(events.map(({ type }) => type));

    injectChaos();

    for (const task of workflow) {
      injectChaos();

      await task(event_types);

      await new Promise((resolve) => setTimeout(resolve, delay));
    }

    injectChaos();

    await TaskManager.dispatchEvent({
      type: ":FINAL",
      payload: { msg },
    });

    TaskManager.log(`${new Date()}: done`);

    TaskManager.shutdown();
  } catch (err) {
    const payload = {
      err_msg: err.message,
      timestamp: new Date().toISOString(),
    };

    TaskManager.dispatchEvent({ type: ":ERROR", payload });

    throw err;
  }
}

main();
