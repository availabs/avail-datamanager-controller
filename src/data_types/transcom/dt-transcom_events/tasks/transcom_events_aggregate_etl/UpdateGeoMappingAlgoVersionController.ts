import { PgEnv } from "../../domain/PostgreSQLTypes";

import TranscomEventsAggregateEtlController from "./TranscomEventsAggregateEtlController";

export default class TranscomEventsGeoMappingOnlyController extends TranscomEventsAggregateEtlController {
  // Does not take eventsStartTime or eventsEndTime. Super will use defaults.
  constructor(protected readonly pgEnv: PgEnv) {
    super(pgEnv);
  }

  protected get initialEtlControlMetadata() {
    const superMetadata = super.initialEtlControlMetadata;

    return {
      ...superMetadata,
      etlTask: "UPDATE_TRANSCOM_EVENTS_GEO_MAPPING_ALGO_VERSION",
    };
  }

  protected async dropOldAndRename() {
    const db = await this.getDbConnection();

    const q = `
      ALTER TABLE _transcom_admin.transcom_events_onto_conflation_map_v0_0_1
        NO INHERIT transcom.transcom_events_onto_conflation_map
      ;

      DROP TABLE transcom.transcom_events_onto_conflation_map ;

      CREATE TABLE transcom.transcom_events_onto_conflation_map (
        LIKE _transcom_admin.transcom_events_onto_conflation_map_v0_0_2 INCLUDING ALL
      ) ;

      ALTER TABLE _transcom_admin.transcom_events_onto_conflation_map_v0_0_2
        NO INHERIT transcom.transcom_events_onto_conflation_map_v2
      ;

      ALTER TABLE _transcom_admin.transcom_events_onto_conflation_map_v0_0_2
        INHERIT transcom.transcom_events_onto_conflation_map
      ;
    `;

    await db.query(q);
  }

  async run() {
    // this.initializeLogging();
    try {
      this.initializeTranscomDatabaseTables();

      await this.initializeDbControlTableEntry();

      await this.beginAggregateUpdateTransaction();

      await this.dropOldAndRename();

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
