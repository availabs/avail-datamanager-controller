import dedent from "dedent";
import pgFormat from "pg-format";

import dama_db from "data_manager/dama_db";
import logger from "data_manager/logger";

import {
  getNpmrdsNetworkNodeIncidentEdgesMetadataInfo,
  getNpmrdsNetworkConformalMatchesTableInfo,
} from "../utils";

export default async function matchingFirstPass(
  year_a: number,
  year_b: number
) {
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

          match_class       TEXT, -- ALTER TO NOT NULL WHEN DONE

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
            -- Call the incident_edges_conformal_matches FUNCTION for the two years.
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
