/* eslint-disable max-len */

import { readFileSync } from "fs";
import { join } from "path";

import dotenv from "dotenv";
import _ from "lodash";

import {
  Client,
  Pool,
  Connection,
  PoolClient,
  QueryConfig,
  QueryResult,
} from "pg";

const configDir = join(__dirname, "../../../../config/");

export type NodePgClient = Client;
export type NodePgPool = Pool;
export type NodePgConnection = Connection;
export type NodePgPoolClient = PoolClient;
export type NodePgQueryConfig = QueryConfig;
export type NodePgQueryResult = QueryResult;
export type PgEnv = string;

// FIXME: Rename to PostgreEnvVariables
export type PsqlConfig = {
  PGHOST?: string;
  PGHOSTADDR?: string;
  PGPORT?: string | number;
  PGDATABASE?: string;
  PGUSER?: string;
  PGPASSWORD?: string;
  PGPASSFILE?: string;
  PGSERVICE?: string;
  PGSERVICEFILE?: string;
  PGOPTIONS?: string;
  PGAPPNAME?: string;
  PGSSLMODE?: string;
  PGREQUIRESSL?: string;
  PGSSLCOMPRESSION?: string;
  PGSSLCERT?: string;
  PGSSLKEY?: string;
  PGSSLROOTCERT?: string;
  PGSSLCRL?: string;
  PGREQUIREPEER?: string;
  PGKRBSRVNAME?: string;
  PGGSSLIB?: string;
  PGCONNECT_TIMEOUT?: string;
  PGCLIENTENCODING?: string;
  PGTARGETSESSIONATTRS?: string;
  PGDATESTYLE?: string;
  PGTZ?: string;
  PGGEQO?: string;
  PGSYSCONFDIR?: string;
  PGLOCALEDIR?: string;
};

export type NodePgConfig = {
  user?: string;
  host?: string;
  database?: string;
  password?: string;
  port?: number;
};

// NOTE: config file naming convention required.
export const getPostgresConfigurationFilePath = (pgEnv: PgEnv) => {
  return join(configDir, `postgres.${pgEnv}.env`);
};

export const postgresEnvVariables = {
  PGHOST: "the host connection parameter",

  PGHOSTADDR:
    "the hostaddr connection parameter. This can be set instead of or in addition to PGHOST to avoid DNS lookup overhead",

  PGPORT: "the port connection parameter",

  PGDATABASE: "the dbname connection parameter",

  PGUSER: "the user connection parameter",

  PGPASSWORD:
    "the password connection parameter. Use of this environment variable is not recommended for security reasons, as some operating systems allow non-root users to see process environment variables via ps; instead consider using a password file (see Section 34.15)",

  PGPASSFILE: "the passfile connection parameter",

  PGSERVICE: "the service connection parameter",

  PGSERVICEFILE:
    "specifies the name of the per-user connection service file. If not set, it defaults to ~/.pg_service.conf (see Section 34.16)",

  PGOPTIONS: "the options connection parameter",

  PGAPPNAME: "the application_name connection parameter",

  PGSSLMODE: "the sslmode connection parameter",

  PGREQUIRESSL:
    "the requiressl connection parameter. This environment variable is deprecated in favor of the PGSSLMODE variable; setting both variables suppresses the effect of this one",

  PGSSLCOMPRESSION: "the sslcompression connection parameter",

  PGSSLCERT: "the sslcert connection parameter",

  PGSSLKEY: "the sslkey connection parameter",

  PGSSLROOTCERT: "the sslrootcert connection parameter",

  PGSSLCRL: "the sslcrl connection parameter",

  PGREQUIREPEER: "the requirepeer connection parameter",

  PGKRBSRVNAME: "the krbsrvname connection parameter",

  PGGSSLIB: "the gsslib connection parameter",

  PGCONNECT_TIMEOUT: "the connect_timeout connection parameter",

  PGCLIENTENCODING: "the client_encoding connection parameter",

  PGTARGETSESSIONATTRS: "the target_session_attrs connection parameter",

  PGDATESTYLE:
    "sets the default style of date/time representation. (Equivalent to SET datestyle TO ....",

  PGTZ: "sets the default time zone. (Equivalent to SET timezone TO ....",

  PGGEQO:
    "sets the default mode for the genetic query optimizer. (Equivalent to SET geqo TO ....",

  PGSYSCONFDIR:
    "sets the directory containing the pg_service.conf file and in a future version possibly other system-wide configuration files",

  PGLOCALEDIR:
    "sets the directory containing the locale files for message localization",
};

export const getPsqlCredentials = (pgEnv: PgEnv): PsqlConfig => {
  const configPath = getPostgresConfigurationFilePath(pgEnv);
  const configContents = readFileSync(configPath);

  const envVars = dotenv.parse(configContents);

  return _.pick(envVars, Object.keys(postgresEnvVariables));
};

export function postgresEnvVariablesToNodePgCreds(pgCreds: PsqlConfig) {
  const nodePgCreds = _.mapKeys(pgCreds, (_v, k) => _.lowerCase(k).slice(2));

  return nodePgCreds;
}

export const getNodePgCredentials = (pgEnv: PgEnv) => {
  const pgCreds = getPsqlCredentials(pgEnv);

  console.log(
    JSON.stringify({ ...pgCreds, PGPASSWORD: "x".repeat(10) }, null, 4)
  );

  return postgresEnvVariablesToNodePgCreds(pgCreds);
};

export const getPostgresConnectionString = (pgEnv: PgEnv) => {
  const {
    user = null,
    host = null,
    database = null,
    password = null,
    port = 5432,
  } = getNodePgCredentials(pgEnv);

  const connStr = `host='${host}' user='${user}' dbname='${database}' password='${password}' port='${port}'`;

  return connStr;
};

// Make sure to call db.end() or Node will hang.
export async function getConnectedNodePgClient(
  pgEnv: PgEnv
): Promise<NodePgClient> {
  const nodePgCreds = getNodePgCredentials(pgEnv);

  // console.log(JSON.stringify({ nodePgCreds }, null, 4));

  const db = new Client(nodePgCreds);
  await db.connect();

  return db;
}

// Make sure to call db.end() or Node will hang.
export async function getConnectedNodePgPool(
  pgEnv: PgEnv
): Promise<NodePgPool> {
  const nodePgCreds = getNodePgCredentials(pgEnv);

  // console.log(JSON.stringify({ nodePgCreds }, null, 4));

  const db = new Pool(nodePgCreds);
  await db.connect();

  return db;
}

export const pgEnvCliArgsSpec = {
  pg_env: {
    type: "string",
    demand: false,
    choices: ["development", "production"],
    default: "development",
  },
};
