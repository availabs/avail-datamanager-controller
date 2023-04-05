import { join, relative } from "path";
import { cliArgsSpec as pgCliArgsSpec } from "../../utils/PostgreSQL";

import TranscomEventsAggregateEtlController from "./TranscomEventsAggregateEtlController";
import TranscomEventsAggregateUpdateController from "./TranscomEventsAggregateUpdateController";
import TranscomEventsAggregateNightlyUpdateController from "./TranscomEventsAggregateNightlyUpdateController";
import TranscomEventsAggregateResumeController from "./TranscomEventsAggregateResumeController";

import TranscomEventsGeoMappingOnlyController from "./TranscomEventsGeoMappingOnlyController";

const builder = {
  start_timestamp: Object.assign({
    desc: "Start timestamp for Transcom Events.",
    demand: false,
    type: "string",
    describe:
      "If not provided, the latest timestamp in the transcom_historical_events table is used.",
  }),

  end_timestamp: {
    desc: "End timestamp for Transcom Events.",
    demand: false,
    type: "string",
    describe: "If not provided, the current time is used.",
  },

  ...pgCliArgsSpec,
};

export const load = {
  desc: "Load the TRANSCOM Events and integrate into the database tables.",
  command: "transcom_events_aggregate_load",
  builder,
  async handler({ pg_env, start_timestamp = null, end_timestamp = null }) {
    const ctrlr = new TranscomEventsAggregateEtlController(
      pg_env,
      start_timestamp,
      end_timestamp
    );

    try {
      await ctrlr.run();
    } catch (err) {
      console.error(err);
    }
  },
};

export const update = {
  desc: "Update the TRANSCOM Events and integrate into the database tables.",
  command: "transcom_events_aggregate_update",
  builder,
  async handler({ pg_env, start_timestamp = null, end_timestamp = null }) {
    const ctrlr = new TranscomEventsAggregateUpdateController(
      pg_env,
      start_timestamp,
      end_timestamp
    );

    try {
      await ctrlr.run();
    } catch (err) {
      console.error(err);
    }
  },
};

export const nightly = {
  desc: "Nighly update the TRANSCOM Events and integrate into the database tables.",
  command: "transcom_events_aggregate_nightly_update",
  builder: pgCliArgsSpec,
  async handler({ pg_env }) {
    const ctrlr = new TranscomEventsAggregateNightlyUpdateController(pg_env);

    try {
      await ctrlr.run();
    } catch (err) {
      console.error(err);
    }
  },
};

export const resume = {
  desc: "Resume a TRANSCOM Events integration by loading downloaded events into the database tables. (Exists for recover after a database ETL error. The TRANSCOM Events download MUST have suceeded.)",
  command: "transcom_events_aggregate_resume",
  builder: {
    ...pgCliArgsSpec,

    etl_start: Object.assign({
      desc: "Timestamp for resumed Transcom Events ETL process. It is the timestamp suffix on the end of the ETL work sub-directory found in etl-work-dir at this repository's root.",
      demand: true,
      type: "string",
    }),
  },
  async handler({ pg_env, etl_start }) {
    const ctrlr = new TranscomEventsAggregateResumeController(
      pg_env,
      etl_start
    );

    try {
      await ctrlr.run();
    } catch (err) {
      console.error(err);
    }
  },
};

export const geoOnly = {
  desc: "Only update the TRANSCOM Event geospatial mappings.",
  command: "transcom_events_geo_only",
  builder: pgCliArgsSpec,
  async handler({ pg_env }) {
    const ctrlr = new TranscomEventsGeoMappingOnlyController(pg_env);

    try {
      await ctrlr.run();
    } catch (err) {
      console.error(err);
    }
  },
};
