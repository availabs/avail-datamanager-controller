import { PgEnv } from "../../domain/PostgreSQLTypes";

import TranscomEventsAggregateEtlController from "./TranscomEventsAggregateEtlController";

export default class TranscomEventsGeoMappingOnlyController extends TranscomEventsAggregateEtlController {
  // Does not take eventsStartTime or eventsEndTime. Super will use defaults.
  constructor(protected readonly pgEnv: PgEnv) {
    super(pgEnv);
  }

  protected get initialEtlControlMetadata() {
    const superMetadata = super.initialEtlControlMetadata;

    return { ...superMetadata, etlTask: "UPDATE_TRANSCOM_EVENTS_GEO_MAPPINGS" };
  }

  async run() {
    // this.initializeLogging();
    try {
      this.initializeTranscomDatabaseTables();

      await this.initializeDbControlTableEntry();

      await this.beginAggregateUpdateTransaction();

      await Promise.all([
        this.updateTranscomEventsAdminGeographies(),
        this.updateTranscomEventsToConflationMap(),
      ]);

      await this.commitAggregateUpdateTransaction();

      this.etlEnd = new Date();

      await this.cleanUp();

      console.log("done");
    } catch (err) {
      this.closeDbConnection();
      throw err;
    }
  }
}
