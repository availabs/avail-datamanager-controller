import { join } from "path";

import execa from "execa";
import Database, { Database as SQLiteDb, Statement } from "better-sqlite3";

import { v4 as uuid } from "uuid";

class TasksController {
  public db: SQLiteDb;
  private _insertEventStmt: Statement;
  private _insertTaskStmt: Statement;

  constructor() {
    this.db = new Database(join(__dirname, "db.sqlite3"));

    this.db.exec(`
      BEGIN ;

      CREATE TABLE IF NOT EXISTS tasks (
        task_id       TEXT PRIMARY KEY,
        worker_path   TEXT NOT NULL,
        pid           INTEGER
      ) ;

      CREATE TABLE IF NOT EXISTS events (
        event_id    INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id     TEXT NOT NULL,
        type        TEXT NOT NULL,
        payload     TEXT
      ) ;

      COMMIT ;
    `);

    this._insertEventStmt = this.db.prepare(
      `INSERT INTO events (task_id, type, payload) VALUES (?, ?, ?) ;`
    );

    this._insertTaskStmt = this.db.prepare(
      "INSERT INTO tasks (task_id, worker_path) VALUES (?, ?)"
    );
  }

  private startWorker(task_id: string, worker_path: string, pid?: number) {
    if (pid) {
    }

    return execa.node(worker_path, {
      env: {
        DAMA_TASK_ID: task_id,
      },
      detached: true,
      // stdio: "inherit",
    });
  }

  async spawnTask(worker_path: string, initial_data: any = null) {
    const task_id = uuid();

    this.db.exec("BEGIN;");

    this._insertTaskStmt.run([task_id, worker_path]);

    this._insertEventStmt.run([
      task_id,
      "INITIAL",
      initial_data && JSON.stringify(initial_data),
    ]);

    this.db.exec("COMMIT;");

    const worker_process = this.startWorker(task_id, worker_path);

    return {
      task_id,
      worker_process,
    };
  }

  async getUnfinishedTasks() {
    return this.db
      .prepare(
        `
          SELECT
              task_id,
              worker_path,
              pid
            FROM events
          EXCEPT
          SELECT
              task_id,
              worker_path,
              pid
            FROM events
            WHERE ( type = 'FINAL' )
          ORDER BY task_id
        `
      )
      .all();
  }

  async restartIncompleteTasks() {
    const unfinishedTasks = await this.getUnfinishedTasks();

    console.table(unfinishedTasks);

    for (const { task_id, worker_path } of unfinishedTasks) {
      this.startWorker(task_id, worker_path);
    }
  }
}

export default new TasksController();
