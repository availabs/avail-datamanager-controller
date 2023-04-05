import { join } from "path";

import { PgEnv } from "../../domain/PostgreSQLTypes";

import TranscomEventsAggregateUpdateController from "./TranscomEventsAggregateUpdateController";
import { getDateFromTimestamp } from "../utils/dates";

const etlBaseDir = join(__dirname, "../../../", "etl-work-dirs");

export default class TranscomEventsAggregateResumeController extends TranscomEventsAggregateUpdateController {
  // Does not take eventsStartTime or eventsEndTime. Super will use defaults.
  constructor(
    protected readonly pgEnv: PgEnv,
    protected readonly etlStartTs: string
  ) {
    super(pgEnv);
    this._etlStart = getDateFromTimestamp(etlStartTs);
  }

  protected get etlStart() {
    if (this._etlStart) {
      return this._etlStart;
    }

    this._etlStart = new Date();

    return this._etlStart;
  }

  protected get etlWorkDir() {
    if (this._etlWorkDir) {
      return this._etlWorkDir;
    }

    const workDirName = `transcom_events_aggregate_etl.${this.etlStartTs}`;

    // @ts-ignore
    this._etlWorkDir = join(etlBaseDir, workDirName);

    this.etlTranscomEventsDir = join(this.etlWorkDir, "transcom-events");

    this.etlTranscomEventsExpandedDir = join(
      this.etlWorkDir,
      "transcom-events-expanded"
    );

    return this._etlWorkDir;
  }

  protected get initialEtlControlMetadata() {
    const superMetadata = super.initialEtlControlMetadata;

    return { ...superMetadata, etlTask: "RESUME_UPDATE_TRANSCOM_EVENTS" };
  }

  async initializeRequestedTranscomEventsDateExtent() {
    console.log("skipping initializeRequestedTranscomEventsDateExtent");
  }

  async downloadTranscomEvents() {
    console.log("skipping downloadTranscomEvents");
  }

  async downloadTranscomEventsExpanded() {
    console.log("skipping downloadTranscomEventsExpanded");
  }

  protected async cleanUp() {
    await this.updateDbControlTableEntry(["status"], "DONE");
    await this.updateDbControlTableEntry(["etlEnd"], this.etlEnd);
    await this.closeDbControlTableEntry();

    await this.closeDbConnection();
    // this.disableLogging();

    // This method override keeps the etlWorkDir
    // rmSync(this.etlWorkDir, { recursive: true });
  }
}
