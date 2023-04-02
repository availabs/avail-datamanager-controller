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

export function getContext() {
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

export const runInDamaContext = (store: EtlContext, fn: () => unknown) =>
  dama_context_async_local_storage.run(store, fn);
