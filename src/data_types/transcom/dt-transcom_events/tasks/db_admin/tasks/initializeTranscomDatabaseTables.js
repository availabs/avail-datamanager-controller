#!/usr/bin/env node

const { execSync } = require("child_process");
const { join } = require("path");

const yargs = require("yargs");

const {
  getPsqlCredentials,
  cliArgsSpec,
} = require("../../../utils/PostgreSQL");

const sqlDir = join(__dirname, "../sql");

function initializeTranscomDatabaseTables(pgEnv) {
  console.log(pgEnv);
  const pgCreds = getPsqlCredentials(pgEnv);

  try {
    execSync(
      `
        psql \
          --quiet \
          -v ON_ERROR_STOP=1 \
          -c 'BEGIN ;' \
          -f '${join(sqlDir, "update_modified_timestamp_trigger_fn.sql")}' \
          -f '${join(sqlDir, "create_admin_etl_control_table.sql")}' \
          -f '${join(sqlDir, "create_congestion_data_table.sql")}' \
          -f '${join(
            sqlDir,
            "create_nysdot_transcom_event_classifications.sql"
          )}' \
          -f '${join(sqlDir, "create_transcom_events_expanded.sql")}' \
          -f '${join(
            sqlDir,
            "create_transcom_event_administative_geographies.sql"
          )}' \
          -f '${join(sqlDir, "create_transcom_events_aggregate.sql")}' \
          -f '${join(
            sqlDir,
            "create_transcom_event_administative_geographies.sql"
          )}' \
          -f '${join(
            sqlDir,
            "update_data_manager_transcom_events_aggregate_statistics_proc.sql"
          )}' \
          -f '${join(
            sqlDir,
            "create_nysdot_transcom_event_classifications.sql"
          )}' \
          -c 'COMMIT ;'
      `,
      {
        env: {
          ...process.env,
          ...pgCreds,
          PGOPTIONS: "--client-min-messages=warning",
        },
        encoding: "utf8",
      }
    );
  } catch (err) {
    // console.error(err.stderr);
  }
}

if (!module.parent) {
  const { argv } = yargs
    .strict()
    .parserConfiguration({
      "camel-case-expansion": false,
      "flatten-duplicate-arrays": false,
    })
    .wrap(yargs.terminalWidth() / 1.618)
    // @ts-ignore
    .option(cliArgsSpec);

  initializeTranscomDatabaseTables(argv.pg_env);
} else {
  module.exports = initializeTranscomDatabaseTables;
}
