import { readFile as readFileAsync } from "fs/promises";
import { join } from "path";

import dama_db from "data_manager/dama_db";
import logger from "data_manager/logger";

export default async function createOverlapsTable(year: number) {
  const sql_fpath = join(__dirname, "./sql/overlaps.sql");

  const template_sql = await readFileAsync(sql_fpath, {
    encoding: "utf8",
  });

  const sql = template_sql.replace(/:YEAR/g, `${year}`);

  logger.debug(
    `Create npmrds_network_path_overlaps_${year}: START ${new Date().toISOString()}`
  );

  await dama_db.query(sql);

  logger.debug(
    `Create npmrds_network_path_overlaps_${year}:  DONE ${new Date().toISOString()}`
  );
}
