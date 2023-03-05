require("ts-node").register();

import TaskManager from "./TaskManager";

const CHAOS_FACTOR = 0;
// const CHAOS_FACTOR = 0.1;

function injectChaos() {
  if (Math.random() < CHAOS_FACTOR) {
    process.exit();
  }
}

async function doTask(type, event_types) {
  injectChaos();

  if (event_types.has(type)) {
    return;
  }

  TaskManager.dispatchEvent(type);
}

const foo = doTask.bind(null, "FOO");
const bar = doTask.bind(null, "BAR");
const baz = doTask.bind(null, "BAZ");

const workflow = [foo, bar, baz];

async function main() {
  try {
    await TaskManager.ready;

    const isDone = await TaskManager.isDone();

    if (isDone) {
      TaskManager.shutdown();
    }

    injectChaos();

    const { delay } = await TaskManager.getInitalEventPayload();

    injectChaos();

    const event_types = await TaskManager.getSeenEventTypes();

    injectChaos();

    for (const task of workflow) {
      injectChaos();

      await task(event_types);

      await new Promise((resolve) => setTimeout(resolve, delay));
    }

    injectChaos();

    await TaskManager.finish();
  } catch (err) {
    const payload = {
      err_msg: err.message,
      timestamp: new Date().toISOString(),
    };

    TaskManager.dispatchEvent("ERROR", payload);
  }
}

main();
