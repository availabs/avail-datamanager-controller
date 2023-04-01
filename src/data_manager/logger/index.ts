// Should each context should have a logger?

import { join } from "path";

import winston from "winston";
import memoize from "memoize-one";

import log_dir from "../../constants/logsDir";

import { getPgEnv, getEtlContextId } from "../contexts";

const getDefaultFileTransport = memoize(() => {
  const fname = `task.${getPgEnv()}.elt_context_${getEtlContextId()}.log`;
  const fpath = join(log_dir, fname);

  return new winston.transports.File({
    filename: fpath,
    format: winston.format.simple(),
  });
});

export enum LoggingLevel {
  error = "error",
  warn = "warn",
  info = "info",
  // http = "http",
  verbose = "verbose",
  debug = "debug",
  silly = "silly",
}

export function setLoggingLevel(level = LoggingLevel.info) {
  winston.level = level;
}

export function addFileTransport(file = getDefaultFileTransport()) {
  winston.add(file);
}

export function removeFileTransport(file = getDefaultFileTransport()) {
  winston.remove(file);
}

const default_console_transport = new winston.transports.Console({
  format: winston.format.cli(),
});

export function addConsoleTransport(console = default_console_transport) {
  winston.add(console);
}

export function removeConsoleTransport(console = default_console_transport) {
  winston.remove(console);
}

export const default_console_logger = winston.createLogger({
  transports: [
    new winston.transports.Console({ format: winston.format.cli() }),
  ],
  level: process.env.AVAIL_LOGGING_LEVEL || "info",
});

export default winston;
