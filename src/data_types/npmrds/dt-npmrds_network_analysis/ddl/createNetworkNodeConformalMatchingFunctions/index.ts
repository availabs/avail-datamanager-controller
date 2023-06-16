import { readFile as readFileAsync } from "fs/promises";
import { join } from "path";

import dedent from "dedent";
import pgFormat from "pg-format";

import dama_db from "data_manager/dama_db";
import logger from "data_manager/logger";

import {
  network_spatial_analysis_schema_name,
  getNpmrdsNetworkNodeIncidentEdgesMetadataInfo,
  getNpmrdsNetworkConformalMatchesTableInfo,
} from "../utils";

const min_year = 2017;
const max_year = 2022;

async function createConformalMatchingFunction() {
  const template_sql_fpath = join(
    __dirname,
    "./sql/create_conformal_matching_fn.sql"
  );

  const template_sql = await readFileAsync(template_sql_fpath, {
    encoding: "utf8",
  });

  const sql = template_sql.replace(
    /__NETWORK_SPATIAL_ANALYSIS_SCHEMA_NAME__/g,
    network_spatial_analysis_schema_name
  );

  await dama_db.query(sql);
  logger.debug("Create level_1_conformal_matches: DONE");
}

async function createConformalMatchesTable(year_a: number, year_b: number) {
  const incident_edges_metdata_a =
    getNpmrdsNetworkNodeIncidentEdgesMetadataInfo(year_a);

  const incident_edges_metdata_b =
    getNpmrdsNetworkNodeIncidentEdgesMetadataInfo(year_b);

  const conformal_matches_info = getNpmrdsNetworkConformalMatchesTableInfo(
    year_a,
    year_b
  );

  const create_table_sql = dedent(
    pgFormat(
      `
        DROP TABLE IF EXISTS %I.%I CASCADE ;

        CREATE TABLE %I.%I (
          node_id_a         INTEGER UNIQUE,
          node_id_b         INTEGER UNIQUE,
          label             JSONB NOT NULL,
          label_fields      JSONB NOT NULL,
          conformal_level   TEXT NOT NULL,

          PRIMARY KEY (node_id_a, node_id_b)
        ) WITH (fillfactor=100)
      `,
      conformal_matches_info.table_schema,
      conformal_matches_info.table_name,
      conformal_matches_info.table_schema,
      conformal_matches_info.table_name
    )
  );

  await dama_db.query(create_table_sql);

  const load_sql = dedent(
    pgFormat(
      `
        INSERT INTO %I.%I (
          node_id_a,
          node_id_b,
          label,
          label_fields,
          conformal_level
        )
          SELECT
              node_id_a,
              node_id_b,
              label,
              label_fields,
              conformal_level
            FROM npmrds_network_spatial_analysis.incident_edges_conformal_matches(
              %L,
              %L
            )
      `,
      conformal_matches_info.table_schema,
      conformal_matches_info.table_name,
      `${incident_edges_metdata_a.table_schema}.${incident_edges_metdata_a.table_name}`,
      `${incident_edges_metdata_b.table_schema}.${incident_edges_metdata_b.table_name}`
    )
  );

  logger.debug(
    `Load ${
      conformal_matches_info.table_name
    } START: ${new Date().toISOString()}`
  );

  await dama_db.query(load_sql);

  logger.debug(
    `Load ${
      conformal_matches_info.table_name
    } DONE: ${new Date().toISOString()}`
  );
}

async function createConformalMatchesTables() {
  for (let year_a = min_year; year_a < max_year; ++year_a) {
    for (let year_b = year_a + 1; year_b <= max_year; ++year_b) {
      await createConformalMatchesTable(year_a, year_b);
    }
  }
}

export default async function createNetworkNodeLevel1Labels() {
  await createConformalMatchingFunction();
  await createConformalMatchesTables();
}
