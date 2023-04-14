import dama_db from "data_manager/dama_db";
import { join } from "path";

const updateTranscomEventsOntoConflationMapSql = join(
  __dirname,
  "./sql/update_transcom_events_onto_conflation_map.sql"
);

const updateTranscomEventsOntoRoadNetworkSql = join(
  __dirname,
  "./sql/update_transcom_events_onto_road_network.sql"
);

const updateTranscomEventsByTmcSummarySql = join(
  __dirname,
  "./sql/update_transcom_events_by_tmc_summary.sql"
);

const updateTranscomEventsTopLevelViewsSql = join(
  __dirname,
  "./sql/update_transcom_events_top_level_views.sql"
);

export default async function main() {
  await dama_db.executeSqlFile(updateTranscomEventsOntoConflationMapSql);

  await dama_db.executeSqlFile(updateTranscomEventsOntoRoadNetworkSql);

  await dama_db.executeSqlFile(updateTranscomEventsByTmcSummarySql);

  await dama_db.executeSqlFile(updateTranscomEventsTopLevelViewsSql);
}
