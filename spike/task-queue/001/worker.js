import { existsSync, readFileSync, appendFileSync } from "fs";
import { join } from "path";

import Database from "better-sqlite3";

const db = new Database(join(__dirname, "db.sqlite3"));

const killOdds = 0.25;

const {
  pid,
  env: { DAMA_TASK_ID },
} = process;

if (Math.random() < killOdds) {
  process.exit();
}

async function main() {
  if (Math.random() < killOdds) {
    process.exit();
  }

  const oldPid = db
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

  if (oldPid) {
    const fpath = `/proc/${oldPid}/environ`;

    if (existsSync(fpath)) {
      const envs = readFileSync(fpath, { encoding: "utf8" });

      const split = envs.split("\0");

      const oldDamaTaskId = split
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
          `${DAMA_TASK_ID}: DAMA_TASK_ID <> PID \n`
        );
      }
    } else {
      appendFileSync(join(__dirname, "log"), `${DAMA_TASK_ID}: not running\n`);
    }
  }

  db.prepare(
    `
      UPDATE tasks
        SET pid = ?
        WHERE ( task_id = ? )
    `
  ).run([pid, DAMA_TASK_ID]);

  if (Math.random() < killOdds) {
    process.exit();
  }

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
      `
    )
    .raw()
    .get([DAMA_TASK_ID]);

  const { count, delay } = JSON.parse(payload);

  let pings = db
    .prepare(
      `
        SELECT
            COUNT(1)
          FROM events
          WHERE (
            ( type = 'PING' )
            AND
            ( task_id = ? )
          )
      `
    )
    .pluck()
    .get([DAMA_TASK_ID]);

  const insertStmt = db.prepare(`
    INSERT INTO events (task_id, type) VALUES (?, ?) ;
  `);

  while (pings < count) {
    if (Math.random() < killOdds) {
      process.exit();
    }

    insertStmt.run([DAMA_TASK_ID, "PING"]);

    ++pings;

    await new Promise((resolve) => setTimeout(resolve, delay));
  }

  insertStmt.run([DAMA_TASK_ID, "FINAL"]);
}

main();
