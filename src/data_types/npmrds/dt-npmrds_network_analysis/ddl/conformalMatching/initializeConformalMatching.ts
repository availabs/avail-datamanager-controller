import { readFile as readFileAsync } from "fs/promises";
import { join } from "path";

import dama_db from "data_manager/dama_db";
import logger from "data_manager/logger";

import { network_spatial_analysis_schema_name } from "../utils";

export default async function initializeConformalMatching() {
  const template_sql_fpath = join(
    __dirname,
    "./sql/create_incident_edges_conformal_matches_fn.sql"
  );

  const template_sql = await readFileAsync(template_sql_fpath, {
    encoding: "utf8",
  });

  const sql = template_sql.replace(
    /__NETWORK_SPATIAL_ANALYSIS_SCHEMA_NAME__/g,
    network_spatial_analysis_schema_name
  );

  await dama_db.query(sql);
  logger.debug("Create incident_edges_conformal_matches: DONE");
}
