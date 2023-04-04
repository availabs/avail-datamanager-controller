import { AsyncLocalStorage } from "async_hooks";

import { Logger } from "winston";

import { getLoggerForProcess } from "../logger";
import { PgEnv } from "../dama_db/postgres/PostgreSQL";

export type EtlContext = Record<string, any> & {
  logger?: Logger;
  meta: {
    pgEnv: PgEnv;
    etl_context_id?: number | null;
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
  const {
    // NOTE:  We could get the context-level logger using getPgEnv and getEtlContextId,
    //          but we want to allow developer choice.
    // @ts-ignore
    logger = getLoggerForProcess(),
  } = getContext();

  return logger;
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
export const runInDamaContext = (store: EtlContext, fn: () => unknown) =>
  dama_context_async_local_storage.run(store, fn);

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
