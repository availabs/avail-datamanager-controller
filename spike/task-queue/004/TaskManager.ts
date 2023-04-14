import { appendFileSync } from "fs";
import { join } from "path";

import Database, { Database as SQLiteDb, Statement } from "better-sqlite3";

import { getProcessDamaTaskId } from "./utils/dama_task_processes";

const {
  pid,
  env: { DAMA_TASK_ID },
} = process;

class TaskManager {
  public db: SQLiteDb;
  private _insertEventStmt: Statement;
  public ready: Promise<void>;

  constructor() {
    this.db = new Database(join(__dirname, "db.sqlite3"));

    this._insertEventStmt = this.db.prepare(
      `INSERT INTO events (task_id, type, payload) VALUES (?, ?, ?) ;`
    );

    this.ready = this.handleStart();
  }

  shutdown() {
    process.exit();
  }

  async isDone() {
    const event_types = await this.getSeenEventTypes();

    return event_types.has("FINAL");
  }

  async handleStart() {
    const old_pid = await this.getLastProcessIdForTask();

    await this.exitIfTaskRunningInAnotherProcess(old_pid);

    this.db.exec("BEGIN ;");

    if (old_pid) {
      await this.dispatchEvent("RESTART", {
        old_pid,
        new_pid: pid,
      });
    }

    this.db
      .prepare(
        `
      UPDATE tasks
        SET pid = ?
        WHERE ( task_id = ? )
    `
      )
      .run([pid, DAMA_TASK_ID]);

    this.db.exec("COMMIT ;");
  }

  private async getLastProcessIdForTask() {
    return this.db
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

  async dispatchEvent(type: string, payload = null) {
    await this.ready;

    payload = payload && JSON.stringify(payload);

    this._insertEventStmt.run([DAMA_TASK_ID, type, payload]);
  }

  async getInitalEventPayload() {
    await this.ready;

    const payload = this.db
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

  async getSeenEventTypes() {
    await this.ready;

    return new Set(
      this.db
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

  private async exitIfTaskRunningInAnotherProcess(old_pid: number) {
    try {
      const old_dama_task_id = getProcessDamaTaskId(old_pid);

      const isStillRunning = DAMA_TASK_ID === old_dama_task_id;

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

  async finish(doneData = null) {
    this.dispatchEvent("FINAL", doneData);
  }
}

export default new TaskManager();
