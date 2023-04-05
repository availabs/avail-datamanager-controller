/*
    await db.query(`
      CALL _transcom_admin.update_transcom_events_onto_conflation_map() ;
      CALL _transcom_admin.update_transcom_events_onto_road_network() ;
      CALL _transcom_admin.update_transcom_events_by_tmc_summary();
      CALL _transcom_admin.update_transcom_events_top_level_views() ;
    `);
*/

import { readFileSync } from "fs";
import { join } from "path";

import AbstractSubprocessController from "../AbstractEtlSubprocessController";

const updateTranscomEventsOntoConflationMapSql = readFileSync(
  join(__dirname, "./sql/update_transcom_events_onto_conflation_map.sql"),
  { encoding: "utf8" }
);

const updateTranscomEventsOntoRoadNetworkSql = readFileSync(
  join(__dirname, "./sql/update_transcom_events_onto_road_network.sql"),
  { encoding: "utf8" }
);

const updateTranscomEventsByTmcSummarySql = readFileSync(
  join(__dirname, "./sql/update_transcom_events_by_tmc_summary.sql"),
  { encoding: "utf8" }
);

const updateTranscomEventsTopLevelViewsSql = readFileSync(
  join(__dirname, "./sql/update_transcom_events_top_level_views.sql"),
  { encoding: "utf8" }
);

export default class TranscomEventsToConflationMapController extends AbstractSubprocessController {
  protected get initialEtlSubProcessControlMetadata() {
    return {
      etlTask: "TranscomEventsToConflationMapController",
      etlStart: this.etlStart,
      status: "IN_PROGRESS",
    };
  }

  protected async updateTranscomEventsOntoConflationMap() {
    return await this.db.query(updateTranscomEventsOntoConflationMapSql);
  }

  protected async updateTranscomEventsOntoRoadNetwork() {
    return await this.db.query(updateTranscomEventsOntoRoadNetworkSql);
  }

  protected async updateTranscomEventsByTmcSummary() {
    return await this.db.query(updateTranscomEventsByTmcSummarySql);
  }

  protected async updateTranscomEventsTopLevelViews() {
    return await this.db.query(updateTranscomEventsTopLevelViewsSql);
  }

  async run() {
    this.etlSubprocessIdx =
      await this.initializeSubprocessControlMetadataEntry();

    console.time("updateTranscomEventsOntoConflationMap");
    await this.updateTranscomEventsOntoConflationMap();
    console.timeEnd("updateTranscomEventsOntoConflationMap");

    console.time("updateTranscomEventsOntoRoadNetwork");
    await this.updateTranscomEventsOntoRoadNetwork();
    console.timeEnd("updateTranscomEventsOntoRoadNetwork");

    console.time("updateTranscomEventsByTmcSummary");
    await this.updateTranscomEventsByTmcSummary();
    console.timeEnd("updateTranscomEventsByTmcSummary");

    console.time("updateTranscomEventsTopLevelViews");
    await this.updateTranscomEventsTopLevelViews();
    console.timeEnd("updateTranscomEventsTopLevelViews");

    await this.finializeSubprocessControlMetadataEntry(this.etlSubprocessIdx);
  }
}
