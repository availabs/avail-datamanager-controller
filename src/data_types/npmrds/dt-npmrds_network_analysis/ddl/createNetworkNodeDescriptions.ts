import pgFormat from "pg-format";
import dedent from "dedent";

import dama_db from "data_manager/dama_db";

import {
  getNpmrdsNetworkNodeIncidentEdgesMetadataInfo,
  getNpmrdsNetworkNodeDescriptionsInfo,
} from "./utils";

export default async function createView(year: number) {
  const incident_edges_meta_info =
    getNpmrdsNetworkNodeIncidentEdgesMetadataInfo(year);

  const node_descriptions_info = getNpmrdsNetworkNodeDescriptionsInfo(year);

  const sql = dedent(
    pgFormat(
      `
        DROP VIEW IF EXISTS %I.%I ;

        CREATE VIEW %I.%I       -- npmrds_network_node_descriptions
          AS
            SELECT
                node_id,

                array_agg(DISTINCT linear_id ORDER BY linear_id) AS linear_ids,

                array_agg(DISTINCT tmc ORDER BY tmc) AS tmcs,

                array_agg(DISTINCT firstname ORDER BY firstname)
                  FILTER (WHERE firstname IS NOT NULL ) AS firstnames,

                jsonb_agg(DISTINCT
                  jsonb_build_object(
                    'tmc',            tmc,
                    'roadnumber',     roadnumber,
                    'roadname',       roadname,
                    'direction',      direction,
                    'bearing',        bearing
                  )
                ) FILTER (WHERE traversal_direction = 'INBOUND') AS inbound_edges,

                jsonb_agg(DISTINCT
                  jsonb_build_object(
                    'tmc',            tmc,
                    'roadnumber',     roadnumber,
                    'roadname',       roadname,
                    'direction',      direction,
                    'bearing',        bearing
                  )
                ) FILTER (WHERE traversal_direction = 'OUTBOUND') AS outbound_edges

              FROM %I.%I          -- npmrds_network_node_incident_edges_metadata

              GROUP BY node_id
        ;
      `,

      //  DROP VIEW
      node_descriptions_info.table_schema,
      node_descriptions_info.table_name,

      //  CREATE VIEW
      node_descriptions_info.table_schema,
      node_descriptions_info.table_name,

      //  FROM AS a
      incident_edges_meta_info.table_schema,
      incident_edges_meta_info.table_name
    )
  );

  await dama_db.query(sql);
}
