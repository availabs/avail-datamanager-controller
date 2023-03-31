import { AsyncLocalStorage } from "async_hooks";

import { PgEnv } from "../dama_db/postgres/PostgreSQL";

export type EtlContext = Record<string, any>;

const dama_context_async_local_storage = new AsyncLocalStorage<EtlContext>();

export default class DamaContextAttachedResource {
  get pg_env(): PgEnv {
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

    console.log("==> Got pgEnv from DamaContext:", pgEnv);

    return pgEnv;
  }

  get etl_context_id(): number {
    const ctx = dama_context_async_local_storage.getStore();

    if (!ctx) {
      throw new Error("Unable to get context from dama_local_storage.");
    }

    const {
      // @ts-ignore
      meta: { etl_context_id },
    } = ctx;

    return etl_context_id;
  }
}

export const runInDamaContext = (store: EtlContext, fn: () => unknown) =>
  dama_context_async_local_storage.run(store, fn);
