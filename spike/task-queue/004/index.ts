//  To run, from DamaController root:
//    ./node_modules/.bin/ts-node ./node_modules/.bin/ts-node spike/task-queue/001/index.ts

import { openSync } from "fs";
import { join } from "path";

import execa from "execa";
import Database from "better-sqlite3";

import { v4 as uuid } from "uuid";

const NUM_TASKS = 10;

const db = new Database(join(__dirname, "db.sqlite3"));

const workerPath = join(__dirname, "./worker.js");

openSync(join(__dirname, "./log"), "a");

db.exec(`
  CREATE TABLE IF NOT EXISTS tasks (
    task_id     TEXT PRIMARY KEY,
    pid         INTEGER
  ) ;

  CREATE TABLE IF NOT EXISTS events (
    event_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     TEXT NOT NULL,
    type        TEXT NOT NULL,
    payload     TEXT
  ) ;
`);

const insertTaskStmt = db.prepare("INSERT INTO tasks (task_id) VALUES (?)");

const insertEventStmt = db.prepare(`
  INSERT INTO events (task_id, type, payload)
    VALUES (?, ?, ?)
`);

function startWorker(task_id: string) {
  execa.node(workerPath, {
    env: {
      AVAIL_DAMA_TASK_ID: task_id,
    },
    detached: true,
    // stdio: "inherit",
  });
}

async function spawnTask(delay = Math.random() * 10000) {
  const task_id = uuid();

  insertTaskStmt.run([task_id]);
  insertEventStmt.run([task_id, "INITIAL", JSON.stringify({ delay })]);

  startWorker(task_id);
}

function restartIncompleteTasks() {
  const undoneTaskIds = db
    .prepare(
      `
        SELECT
            task_id
          FROM events
        EXCEPT
        SELECT
            task_id
          FROM events
          WHERE ( type = 'FINAL' )
        ORDER BY task_id
      `
    )
    .all();

  console.table(undoneTaskIds);

  for (const { task_id } of undoneTaskIds) {
    startWorker(task_id);
  }
}

restartIncompleteTasks();

for (let i = 0; i < NUM_TASKS; ++i) {
  spawnTask();
}
