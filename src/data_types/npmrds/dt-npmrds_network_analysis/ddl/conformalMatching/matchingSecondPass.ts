import { readFile as readFileAsync } from "fs/promises";
import { join } from "path";

import dama_db from "data_manager/dama_db";
import logger from "data_manager/logger";

const PT_DIFF_METERS = 15;

export default async function matchingSecondPass(
  year_a: number,
  year_b: number
) {
  logger.debug(
    `Run matchingSecondPass for ${year_a} & ${year_b}: START ${new Date().toISOString()}`
  );

  const sql_fpath = join(__dirname, "./sql/matching_second_pass.sql");

  const template_sql = await readFileAsync(sql_fpath, {
    encoding: "utf8",
  });

  const sql = template_sql
    .replace(/:YEAR_A/g, `${year_a}`)
    .replace(/:YEAR_B/g, `${year_b}`)
    .replace(/:PT_DIFF_METERS/g, `${PT_DIFF_METERS}`);

  await dama_db.query(sql);

  logger.debug(
    `Run matchingSecondPass for ${year_a} & ${year_b}: DONE ${new Date().toISOString()}`
  );
}
