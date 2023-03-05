import { readFileSync, appendFileSync } from "fs";
import { join } from "path";

import Database from "better-sqlite3";

const db = new Database(join(__dirname, "db.sqlite3"));

// const CHAOS_FACTOR = 0;
const CHAOS_FACTOR = 0.1;

const {
  pid,
  env: { DAMA_TASK_ID },
} = process;

function injectChaos() {
  if (Math.random() < CHAOS_FACTOR) {
    process.exit();
  }
}

const insertEventStmt = db.prepare(
  `INSERT INTO events (task_id, type, payload) VALUES (?, ?, ?) ;`
);

async function dispatchEvent(type, payload = null) {
  payload = payload && JSON.stringify(payload);

  insertEventStmt.run([DAMA_TASK_ID, type, payload]);
}

async function doTask(type, event_types) {
  injectChaos();

  if (event_types.has(type)) {
    return;
  }

  await dispatchEvent(type);
}

const foo = doTask.bind(null, "FOO");
const bar = doTask.bind(null, "BAR");
const baz = doTask.bind(null, "BAZ");

const workflow = [foo, bar, baz];

async function getLastProcessIdForTask() {
  return db
    .prepare(
      `
        SELECT
            pid
          FROM tasks
          WHERE ( task_id = ? )
      `
    )
    .pluck()
    .get([DAMA_TASK_ID]);
}

async function getInitalEventPayload() {
  const payload = db
    .prepare(
      `
        SELECT
            payload
          FROM events
          WHERE (
            ( type = 'INITIAL' )
            AND
            ( task_id = ? )
          )
        ;
      `
    )
    .pluck()
    .get([DAMA_TASK_ID]);

  return JSON.parse(payload || {});
}

async function getSeenEventTypes() {
  return new Set(
    db
      .prepare(
        `
          SELECT
              type
            FROM events
            WHERE ( task_id = ? )
          ;
        `
      )
      .pluck()
      .all([DAMA_TASK_ID])
  );
}

async function exitIfTaskRunningInAnotherProcess(old_pid) {
  const fpath = `/proc/${old_pid}/environ`;

  try {
    const envs = readFileSync(fpath, { encoding: "utf8" });

    const oldDamaTaskId = envs
      .split("\0")
      .find((envLine) => /^DAMA_TASK_ID/.test(envLine))
      ?.replace(/.*=/, "");

    const isStillRunning = DAMA_TASK_ID === oldDamaTaskId;

    if (isStillRunning) {
      appendFileSync(
        join(__dirname, "log"),
        `${DAMA_TASK_ID}: still running\n`
      );

      process.exit();
    } else {
      appendFileSync(
        join(__dirname, "log"),
        `${DAMA_TASK_ID}: not running (DAMA_TASK_ID <> PID) \n`
      );
    }
  } catch (err) {
    const { message } = err;

    if (message && /^ENOENT/.test(message)) {
      appendFileSync(
        join(__dirname, "log"),
        `${DAMA_TASK_ID}: not running (NO PROC FILE)\n`
      );
    } else {
      appendFileSync(
        join(__dirname, "log"),
        `${DAMA_TASK_ID}: ${err.message}\n`
      );
    }
  }
}

async function handleStart() {
  const old_pid = await getLastProcessIdForTask(db);

  await exitIfTaskRunningInAnotherProcess(old_pid);

  db.exec("BEGIN ;");

  if (old_pid) {
    await dispatchEvent("RESTART", {
      old_pid,
      new_pid: pid,
    });
  }

  db.prepare(
    `
      UPDATE tasks
        SET pid = ?
        WHERE ( task_id = ? )
    `
  ).run([pid, DAMA_TASK_ID]);

  db.exec("COMMIT ;");
}

async function finish() {
  dispatchEvent("FINAL");
}

async function main() {
  try {
    injectChaos();

    await handleStart();

    injectChaos();

    const { delay } = await getInitalEventPayload();

    injectChaos();

    const event_types = await getSeenEventTypes();

    if (event_types.has("FINAL")) {
      return;
    }

    injectChaos();

    for (const task of workflow) {
      injectChaos();

      await task(event_types);

      await new Promise((resolve) => setTimeout(resolve, delay));
    }

    injectChaos();

    await finish();
  } catch (err) {
    const payload = {
      err_msg: err.message,
      timestamp: new Date().toISOString(),
    };

    await dispatchEvent("ERROR", payload);
  }
}

main();
