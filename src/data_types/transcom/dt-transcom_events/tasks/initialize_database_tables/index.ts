import { join } from "path";

import dama_db from "data_manager/dama_db";
import { getPgEnv } from "data_manager/contexts";
import logger from "data_manager/logger";

const sql_dir = join(__dirname, "./sql");

const fnames = [
  "update_modified_timestamp_trigger_fn.sql",
  "create_congestion_data_table.sql",
  "create_nysdot_transcom_event_classifications.sql",
  "create_transcom_events_expanded.sql",
  "create_transcom_event_administative_geographies.sql",
  "create_transcom_events_aggregate.sql",
  "create_transcom_event_administative_geographies.sql",
  "create_nysdot_transcom_event_classifications.sql",
];

export default async function main(
  etl_context = { meta: { pgEnv: getPgEnv() } }
) {
  const {
    meta: { pgEnv: pg_env },
  } = etl_context;

  await dama_db.runInTransactionContext(async () => {
    for (const fname of fnames) {
      const fpath = join(sql_dir, fname);

      logger.silly(`executing DDL file ${fpath}`);

      await dama_db.executeSqlFile(fpath);

      logger.debug(`executed DDL file ${fpath}`);
    }
  }, pg_env);
}
