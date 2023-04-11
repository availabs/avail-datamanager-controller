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

export function getContext(): EtlContext {
  const ctx = dama_context_async_local_storage.getStore();

  if (!ctx) {
    throw new Error("Unable to get context from dama_local_storage.");
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

    return !!(ctx?.meta?.pgEnv && ctx.meta.etl_context_id && ctx.initial_event);
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
