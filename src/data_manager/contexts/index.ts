/*
 * NOTE:  Could use AVAIL_DAMA_PG_ENV and AVAIL_DAMA_ETL_CONTEXT_ID in getContext.
 *        Could also use require.main to determine if the main module is TaskRunner.
 *
 *          IF (
 *            ( both AVAIL_DAMA_PG_ENV and AVAIL_DAMA_ETL_CONTEXT_ID are set )
 *            AND
 *            ( /\/src\/data_manager\/tasks\/TaskRunner.ts$/.test(require.main.filepath) )
 *          ) THEN
 *              IF dama_context_async_local_storage.getStore() returns undefined
 *                THEN
 *                  getContext returns { meta: { pgEnv: AVAIL_DAMA_PG_ENV, ... } }
 *
 */

import { AsyncLocalStorage } from "async_hooks";
import { inspect } from "util";

import { Logger } from "winston";
import _ from "lodash";

import logger, { getLoggerForProcess } from "data_manager/logger";

import { PgEnv } from "../dama_db/postgres/PostgreSQL";

export type EtlContext = Record<string, any> & {
  logger?: Logger;
  meta: {
    pgEnv: PgEnv;
    etl_context_id?: number | null;
  };
};

export type TaskEtlContext = EtlContext & {
  initial_event: Record<string, any>;
  logger: Logger;
  meta: {
    pgEnv: PgEnv;
    etl_context_id: number;
    parent_context_id?: number;
  };
};

const dama_context_async_local_storage = new AsyncLocalStorage<EtlContext>();

const TESTING_PG_ENV = "ephemeral_test_db";

export function getContext(): EtlContext {
  const ctx = dama_context_async_local_storage.getStore();

  if (!ctx) {
    throw new Error("Unable to get context from dama_local_storage.");
  }

  // Because we drop entire SCHEMAs when testing...
  // https://stackoverflow.com/a/52231746
  if (
    process.env.JEST_WORKER_ID !== undefined ||
    process.env.NODE_ENV === "test"
  ) {
    if (ctx.meta.pgEnv !== TESTING_PG_ENV) {
      throw new Error(`The ONLY pgEnv allowed in testing is ${TESTING_PG_ENV}`);
    }
  }

  return ctx;
}

export function isInEtlContext(): boolean {
  try {
    const ctx = getContext();

    return !!ctx?.meta?.pgEnv;
  } catch (err) {
    return false;
  }
}

export function isInTaskEtlContext(): boolean {
  try {
    const ctx = getContext();

    // return !!(ctx?.meta?.pgEnv && ctx.meta.etl_context_id && ctx.initial_event);
    return !!(ctx?.meta?.pgEnv && ctx.meta.etl_context_id);
  } catch (err) {
    return false;
  }
}

export function verifyIsInTaskEtlContext() {
  if (!isInTaskEtlContext()) {
    throw new Error("MUST run in a TaskEtlContext");
  }
}

export function getPgEnv(): PgEnv {
  const {
    // @ts-ignore
    meta: { pgEnv } = {},
  } = getContext();

  if (!pgEnv) {
    throw new Error("dama_local_storage.meta.pgEnv is not set");
  }

  return pgEnv;
}

export function getEtlContextId(): number | null {
  const {
    // @ts-ignore
    meta: { etl_context_id } = {},
  } = getContext();

  return etl_context_id || null;
}

export function getLogger(): Logger {
  return logger;
}

// For debugging
export function logContextInfo() {
  const proc_logger = getLoggerForProcess();

  try {
    const ctx = getContext();

    proc_logger.info(inspect(_.omit(ctx, "logger")));
  } catch (err) {
    proc_logger.info("Not in a context");
  }
}

export default class DamaContextAttachedResource {
  get pg_env(): PgEnv {
    return getPgEnv();
  }

  get etl_context_id(): number | null {
    return getEtlContextId();
  }

  get logger(): Logger {
    return getLogger();
  }
}

// NOTE: Can use getContext to get the parent_context.
export const runInDamaContext = (store: EtlContext, fn: () => unknown) => {
  // https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object/defineProperty
  // Currently causes the following error:
  //    >> ERROR:
  //    TypeError: Cannot assign to read only property 'pgEnv' of object '#<Object>'
  // Changing the pgEnv could be very bad.
  // Object.defineProperty(store.meta, "pgEnv", {
  //   configurable: false,
  //   writable: false,
  // });

  return dama_context_async_local_storage.run(store, fn);
};

/*
// NOTE: Can use getContext to get the parent_context.
export const runInDamaContext = (store: EtlContext, fn: () => unknown) => {
  const _store_ = _.cloneDeep(store);
  // console.log(inspect({ _store_, store }));

  _store_.meta = _store_.meta || {};

  if (!_store_.meta.pgEnv) {
    try {
      _store_.meta.pgEnv = getPgEnv();
    } catch (err) {
      throw new Error("All EtlContexts MUST have a meta.pgEnv");
    }
  }

  return dama_context_async_local_storage.run(_store_, fn);
};
*/
