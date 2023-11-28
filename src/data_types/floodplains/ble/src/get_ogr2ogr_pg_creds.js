#!/usr/bin/env node

require("ts-node").register();
require("tsconfig-paths").register();

const {
  getPostgresConnectionString,
} = require("data_manager/dama_db/postgres/PostgreSQL");

const pg_env = "dama_dev_1";

console.log(getPostgresConnectionString(pg_env));
