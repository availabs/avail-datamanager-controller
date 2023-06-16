import pgFormat from "pg-format";

import { network_spatial_analysis_schema_name } from "./utils";

export default function createNpmrdsNetworkSpatialAnalysisSchema() {
  const sql = pgFormat(
    "CREATE SCHEMA %I ;",
    network_spatial_analysis_schema_name
  );

  return dama_db.query(sql);
}
