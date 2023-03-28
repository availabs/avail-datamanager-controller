import { AsyncLocalStorage } from "async_hooks";

import { PgEnv } from "../dama_db/postgres/PostgreSQL";

const async_local_storage = new AsyncLocalStorage<Record<string, any>>();

export default class DamaContextAttachedResource {
  get pg_env(): PgEnv {
    const ctx = async_local_storage.getStore();

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
    const ctx = async_local_storage.getStore();

    if (!ctx) {
      throw new Error("Unable to get context from dama_local_storage.");
    }

    const {
      // @ts-ignore
      meta: { etl_context_id },
    } = ctx;

    if (!etl_context_id) {
      throw new Error("dama_local_storage.meta.etl_context_id is not set");
    }

    return etl_context_id;
  }
}

export const runInDamaContext = (store: Map<any, any>, fn: () => unknown) =>
  async_local_storage.run(store, fn);
