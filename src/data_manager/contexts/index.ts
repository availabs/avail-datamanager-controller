import { AsyncLocalStorage } from "async_hooks";

import { Logger } from "winston";

import { default_console_logger } from "../logger";
import { PgEnv } from "../dama_db/postgres/PostgreSQL";

export type EtlContext = Record<string, any>;

const dama_context_async_local_storage = new AsyncLocalStorage<EtlContext>();

export function getPgEnv(): PgEnv {
  const ctx = dama_context_async_local_storage.getStore();

  if (!ctx) {
    throw new Error("Unable to get context from dama_local_storage.");
  }

  const {
    // @ts-ignore
    meta: { pgEnv },
  } = ctx;

  if (!pgEnv) {
    throw new Error("dama_local_storage.meta.pgEnv is not set");
  }

  // console.log("==> Got pgEnv from DamaContext:", pgEnv);

  return pgEnv;
}

export function getEtlContextId(): number {
  const ctx = dama_context_async_local_storage.getStore();

  if (!ctx) {
    throw new Error("Unable to get context from dama_local_storage.");
  }

  const {
    // @ts-ignore
    meta: { etl_context_id },
  } = ctx;

  return etl_context_id || null;
}

export function getLogger(): Logger {
  const ctx = dama_context_async_local_storage.getStore();

  if (!ctx) {
    throw new Error("Unable to get context from dama_local_storage.");
  }

  const {
    // @ts-ignore
    logger = default_console_logger,
  } = ctx;

  return logger;
}

export default class DamaContextAttachedResource {
  get pg_env(): PgEnv {
    return getPgEnv();
  }

  get etl_context_id(): number {
    return getEtlContextId();
  }

  get logger(): Logger {
    return getLogger();
  }
}

export const runInDamaContext = (store: EtlContext, fn: () => unknown) =>
  dama_context_async_local_storage.run(store, fn);
