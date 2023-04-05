import { readFileSync } from "fs";
import { join } from "path";

import AbstractSubprocessController from "../AbstractEtlSubprocessController";

const updateTranscomEventsAdminGeographiesSql = readFileSync(
  join(__dirname, "./sql/update_transcom_event_administative_geographies.sql"),
  { encoding: "utf8" }
);

export default class TranscomEventsToAdminGeographiesSubprocessController extends AbstractSubprocessController {
  protected get initialEtlSubProcessControlMetadata() {
    return {
      etlTask: "TranscomEventsToAdminGeographies",
      etlStart: this.etlStart,
      status: "IN_PROGRESS",
    };
  }

  protected async updateTranscomEventsAdminGeographies() {
    await this.db.query(updateTranscomEventsAdminGeographiesSql);
  }

  async run() {
    this.etlSubprocessIdx =
      await this.initializeSubprocessControlMetadataEntry();

    console.time("updateTranscomEventsAdminGeographies");
    await this.updateTranscomEventsAdminGeographies();
    console.timeEnd("updateTranscomEventsAdminGeographies");

    await this.finializeSubprocessControlMetadataEntry(this.etlSubprocessIdx);
  }
}
