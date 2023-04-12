import { copyFileSync, symlinkSync, readdirSync } from "fs";
import { join } from "path";

import tmp from "tmp";

// import dama_db from "data_manager/dama_db";
import dama_events from "data_manager/events";

import { runInDamaContext } from "data_manager/contexts";

import etl_dir from "constants/etlDir";

import main from ".";
import getEtlWorkDir, { getEtlWorkDirMeta } from "../utils/etlWorkDir";
import getEtlContextLocalStateSqliteDb from "../utils/getEtlContextLocalStateSqliteDb";
import getPostgresStagingSchemaName from "../utils/getPostgresStagingSchemaName";

const test_data_dir = join(
  __dirname,
  "../../test_data/dt-transcom_events.ephemeral_test_db.-1"
);

const PG_ENV = "ephemeral_test_db";

test("loads TRANSCOM events into the database", async () => {
  // We use a tmp dir so there are no name collisions.
  // Recall the ephemeral_test_db database is DROPPED before tests run.
  // That means the same etl_context_ids will reappear.
  const { name: tmp_dir, removeCallback } = tmp.dirSync({
    prefix: "dt-transcom_events.test.",
    tmpdir: etl_dir,
    unsafeCleanup: true,
  });

  try {
    const etl_context_id = await dama_events.spawnEtlContext(
      null,
      null,
      PG_ENV
    );

    const etl_work_dir = getEtlWorkDir(PG_ENV, etl_context_id, tmp_dir);

    const {
      sqlite_db_path: test_sqlite_db_path,
      raw_transcom_events_download_dir_path: test_downloads_dir,
    } = getEtlWorkDirMeta(test_data_dir);

    const {
      sqlite_db_path: etl_sqlite_db_path,
      raw_transcom_events_download_dir_path: etl_downloads_dir,
    } = getEtlWorkDirMeta(etl_work_dir);

    // copy the SQLite database
    copyFileSync(test_sqlite_db_path, etl_sqlite_db_path);

    const sqlite_db = getEtlContextLocalStateSqliteDb(etl_work_dir);

    sqlite_db
      .prepare(
        `
          UPDATE etl_context
            SET pg_env          = ?,
                etl_context_id  = ?,
                staging_schema  = ?
          ;
        `
      )
      .run(
        PG_ENV,
        etl_context_id,
        getPostgresStagingSchemaName(etl_context_id)
      );

    // symlink the downloaded events files
    for (const fname of readdirSync(test_downloads_dir)) {
      const src = join(test_downloads_dir, fname);
      const dest = join(etl_downloads_dir, fname);

      symlinkSync(src, dest);
    }

    const initial_event = { type: ":INITIAL" };

    await dama_events.dispatch(initial_event, etl_context_id, PG_ENV);

    const ctx = { meta: { pgEnv: PG_ENV, etl_context_id } };

    const final_event = await runInDamaContext(ctx, () => main(etl_work_dir));

    expect(final_event).toBeTruthy();
  } finally {
    removeCallback();
  }
});
