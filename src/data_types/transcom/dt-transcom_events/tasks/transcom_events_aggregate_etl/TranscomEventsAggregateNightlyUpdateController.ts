import { PgEnv } from "../../domain/PostgreSQLTypes";

import TranscomEventsAggregateUpdateController from "./TranscomEventsAggregateUpdateController";

export default class TranscomEventsAggregateTranscomEventsAggregateNightlyUpdateController extends TranscomEventsAggregateUpdateController {
  // Does not take eventsStartTime or eventsEndTime. Super will use defaults.
  constructor(protected readonly pgEnv: PgEnv) {
    super(pgEnv);
  }

  protected get initialEtlControlMetadata() {
    const superMetadata = super.initialEtlControlMetadata;

    return { ...superMetadata, etlTask: "NIGHTLY_UPDATE_TRANSCOM_EVENTS" };
  }

  protected async initializeRequestedTranscomEventsDateExtent() {
    super.initializeRequestedTranscomEventsDateExtent();

    // We request the previous days in case of updates.
    this.eventsStartTime.setDate(this.eventsStartTime.getDate() - 1);
    this.eventsStartTime.setHours(0);
    this.eventsStartTime.setMinutes(0);
    this.eventsStartTime.setSeconds(0);
  }
}
