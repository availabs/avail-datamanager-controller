import { join } from "path";

import Cursor from "pg-cursor";

import sqlite, { Database as SQLiteDatabase } from "better-sqlite3";

import { makeRawTranscomEventIteratorFromApiScrapeDirectory } from "../transcom_events";

import TranscomEventsAggregateEtlController from "./TranscomEventsAggregateEtlController";

export default class TranscomEventsAggregateEtlControl extends TranscomEventsAggregateEtlController {
  protected _etlWorkDb: SQLiteDatabase;

  private get etlWorkDb() {
    if (this._etlWorkDb) {
      return this._etlWorkDb;
    }

    const etlWorkDbPath = join(this.etlWorkDir, "etl_work_db.sqlite3");
    this._etlWorkDb = new sqlite(etlWorkDbPath);
    this._etlWorkDb.pragma("journal_mode = WAL");

    this._etlWorkDb.exec(`
      CREATE TABLE transcom_events_expanded_timestamps (
        event_id          TEXT PRIMARY KEY,
        start_date_time   TEXT,
        close_date        TEXT,
        last_updatedate   TEXT
      ) WITHOUT ROWID;
    `);

    return this._etlWorkDb;
  }

  protected async *makeDbTranscomEventTimestampsIter() {
    const sql = `
      SELECT
          event_id,
          start_date_time,
          close_date,
          last_updatedate
        FROM _transcom_admin.transcom_events_expanded
        WHERE (
          ( start_date_time >= $1 )
          AND
          ( start_date_time <= $2 )
        )
        ORDER BY event_id
    `;

    await this.initializeRequestedTranscomEventsDateExtent();

    const cursorRequest = new Cursor(sql, [
      this.eventsStartTime,
      this.eventsEndTime,
    ]);

    const db = await this.getDbConnection();

    const cursor = db.query(cursorRequest);

    const fn = (resolve: Function, reject: Function) => {
      cursor.read(1000, (err, rows) => {
        if (err) {
          return reject(err);
        }

        return resolve(rows);
      });
    };

    while (true) {
      const rows: any[] = await new Promise(fn);

      if (!rows.length) {
        break;
      }

      for (const row of rows) {
        for (const col of [
          "start_date_time",
          "close_date",
          "last_updatedate",
        ]) {
          // For comparison with the RawTranscomEvent timestamps formatted as ISO
          row[col] = row[col] ? new Date(row[col]).toISOString() : "";
        }

        yield row;
      }
    }
  }

  protected async loadLocalTranscomEventTimestampsDb() {
    const etlDbInsrtStmt = this.etlWorkDb.prepare(`
      INSERT INTO transcom_events_expanded_timestamps (
        event_id,
        start_date_time,
        close_date,
        last_updatedate
      ) VALUES (?, ?, ?, ?)
    `);

    this.etlWorkDb.exec("BEGIN;");

    const iter = this.makeDbTranscomEventTimestampsIter();

    for await (const {
      event_id,
      start_date_time,
      close_date,
      last_updatedate,
    } of iter) {
      etlDbInsrtStmt.run(
        event_id,
        start_date_time,
        close_date,
        last_updatedate
      );
    }

    this.etlWorkDb.exec("COMMIT;");
    this.etlWorkDb.exec("VACUUM;");
    this.etlWorkDb.exec("ANALYZE;");
  }

  protected get initialEtlControlMetadata() {
    const superMetadata = super.initialEtlControlMetadata;

    return { ...superMetadata, etlTask: "UPDATE_TRANSCOM_EVENTS" };
  }

  protected async *makeOutdatedTranscomEventsIdsIterator() {
    await this.loadLocalTranscomEventTimestampsDb();

    const iter = makeRawTranscomEventIteratorFromApiScrapeDirectory(
      this.etlTranscomEventsDir
    );

    const isOutdatedStmt = this.etlWorkDb.prepare(`
      SELECT NOT EXISTS (
        SELECT
            1
          FROM transcom_events_expanded_timestamps
          WHERE (
            ( event_id = ? )
            AND
            ( start_date_time = ? )
            AND
            ( close_date = ? )
            AND
            ( last_updatedate = ? )
          )
      )
    `);

    for await (const {
      id,
      startDateTime,
      manualCloseDate,
      lastUpdate,
    } of iter) {
      const start_date_time = startDateTime
        ? new Date(startDateTime).toISOString()
        : "";
      const close_date = manualCloseDate
        ? new Date(manualCloseDate).toISOString()
        : "";
      const last_updatedate = lastUpdate
        ? new Date(lastUpdate).toISOString()
        : "";

      const isOutdated = isOutdatedStmt
        .pluck()
        .get(id, start_date_time, close_date, last_updatedate);

      if (isOutdated) {
        yield id;
      }
    }
  }

  protected get transcomEventIdAsyncIteratorFromApiScrapeDirectory() {
    return this.makeOutdatedTranscomEventsIdsIterator();
  }

  async run() {
    await super.initializeRequestedTranscomEventsDateExtent();
    super.run();
  }
}
