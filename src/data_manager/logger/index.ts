import { join } from "path";
import memoize from "memoize-one";

import winston, { Logger } from "winston";

import { getTimestamp } from "../..//data_utils/time";

import log_dir from "../../constants/logsDir";

import { getPgEnv, getEtlContextId } from "../contexts";

export enum LoggingLevel {
  error = "error",
  warn = "warn",
  info = "info",
  verbose = "verbose",
  debug = "debug",
  silly = "silly",
}

const console_transport = new winston.transports.Console({
  format: winston.format.cli(),
});

// ========== Process-Level Logger ===========

export const getLoggerForProcess = memoize(() => {
  const file_transport = new winston.transports.File({
    filename: join(log_dir, `dama-process.${getTimestamp()}.log`),
    format: winston.format.simple(),
  });

  return winston.createLogger({
    transports: [console_transport, file_transport],
    level: process.env.AVAIL_LOGGING_LEVEL || "info",
  });
});

// ========== Context-Level Loggers ===========

// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/WeakRef
// https://v8.dev/features/weak-references
const etl_context_loggers: Record<string, WeakRef<Logger>> = {};

// Deletes entry from the etl_context_loggers object after Logger is garbage collected.
setInterval(() => {
  for (const k of Object.keys(etl_context_loggers)) {
    if (!etl_context_loggers[k]?.deref()) {
      delete etl_context_loggers[k];
    }
  }
}, 60000);

export function getLoggerForContext(
  etl_context_id = getEtlContextId(),
  pg_env = getPgEnv()
) {
  const k = `pg_env=${pg_env}::etl_context_id=${etl_context_id}`;

  const existing_logger = etl_context_loggers[k]?.deref();

  if (existing_logger) {
    return existing_logger;
  }

  const file_transport = new winston.transports.File({
    filename: join(
      log_dir,
      `dama-context.pg_env.${pg_env}.elt_context.${etl_context_id}.log`
    ),
    format: winston.format.simple(),
  });

  const logger = winston.createLogger({
    transports: [console_transport, file_transport],
    level: "debug",
  });

  const logger_weak_ref = new WeakRef(logger);

  etl_context_loggers[k] = logger_weak_ref;

  return logger;
}
