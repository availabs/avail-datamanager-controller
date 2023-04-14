import { join } from "path";

import dama_db from "data_manager/dama_db";

import { verifyIsInTaskEtlContext } from "data_manager/contexts";

// import getEtlContextLocalStateSqliteDb from "../../../../utils/getEtlContextLocalStateSqliteDb";

const sql_file_path = join(
  __dirname,
  "./sql/update_transcom_event_administative_geographies.sql"
);

export default async function main(etl_work_dir: string) {
  verifyIsInTaskEtlContext();

  await dama_db.executeSqlFile(sql_file_path);

  /*
  const sqlite_db = getEtlContextLocalStateSqliteDb(etl_work_dir);

  const staging_schema = sqlite_db
    .prepare(
      `
        SELECT
            staging_schema
          FROM etl_context
      `
    )
    .pluck()
    .get();
  */
}
