import { readFile as readFileAsync } from "fs/promises";
import { join } from "path";

import _ from "lodash";
import { v4 as uuid } from "uuid";

import Cursor from "pg-cursor";

import DamaContextAttachedResource, {
  EtlContext,
  getContext,
  runInDamaContext,
} from "../contexts/index";

import logger from "data_manager/logger";

import {
  NodePgPool,
  NodePgPoolClient,
  NodePgQueryConfig,
  NodePgQueryResult,
  listAllPgEnvs,
  getConnectedNodePgPool,
} from "./postgres/PostgreSQL";

export type QueryLogEntry = {
  query: NodePgQueryConfig | string;
  result: NodePgQueryResult | string | null;
};

type LocalVariables = {
  // Promise because below we only want to getDb once and this._local_.db is our "once" check.
  dbs: Record<string, Promise<NodePgPool> | null>;
};

// Order matters.
const dbInitializationScripts = [
  "create_required_extensions.sql",
  "create_dama_core_tables.sql",
  "create_dama_etl_context_and_events_tables.sql",
  "create_dama_admin_helper_functions.sql",
  "create_geojson_schema_table.sql",
  "create_dama_table_schema_utils.sql",
  "create_data_source_metadata_utils.sql",
  "create_mbtiles_tables.sql",
];

type DamaDbSingleQueryParam = string | NodePgQueryConfig;
type DamaDbMultiQueryParam = DamaDbSingleQueryParam[];

type DamaDbQueryParamType = DamaDbSingleQueryParam | DamaDbMultiQueryParam;

// https://stackoverflow.com/a/54166010
type DamaDbQueryReturnType<T> = T extends DamaDbSingleQueryParam
  ? NodePgQueryResult
  : NodePgQueryResult[];

export type DamaDbQueryOptions = {
  // Primarily for use by dama_events so events persist even if transaction rollback.
  outside_txn_ctx?: boolean;
};

class DamaDb extends DamaContextAttachedResource {
  private readonly _local_: LocalVariables;

  constructor() {
    super();

    this._local_ = <LocalVariables>{
      dbs: {},
      transactionConnections: {},
    };
  }

  listAllPgEnvs() {
    return listAllPgEnvs();
  }

  /**
   * Returns [NodePgPool](https://node-postgres.com/apis/pool) for the given pg_env.
   *
   * @remarks
   * Private method used to get cached database connection pools.
   *
   * @param pg_env - The database to connect to. Optional if running in an EtlContext.
   */
  private async getDb(pg_env = this.pg_env): Promise<NodePgPool> {
    logger.silly(`dama_db.getDb ${pg_env}`);

    if (!this._local_.dbs[pg_env]) {
      let resolve: Function;

      this._local_.dbs[pg_env] = new Promise((res) => {
        resolve = res;
      });

      const db = await getConnectedNodePgPool(pg_env);
      const dbConnection = await db.connect();

      try {
        await dbConnection.query("BEGIN;");

        for (const scriptFile of dbInitializationScripts) {
          const sqlPath = join(__dirname, "sql", scriptFile);
          const sql = await readFileAsync(sqlPath, { encoding: "utf8" });

          await dbConnection.query(sql);
        }

        dbConnection.query("COMMIT ;");

        process.nextTick(() => resolve(db));
      } catch (err) {
        // FIXME CONSIDER:  Should reject if error rather than resolving null?
        //                  That would break the idempotency check.
        this._local_.dbs[pg_env] = null;

        console.error(err);

        db.end();

        process.nextTick(() => resolve(null));
      } finally {
        try {
          dbConnection.release();
        } catch (err) {
          //
        }
      }
    }

    // @ts-ignore
    return this._local_.dbs[pg_env];
  }

  /**
   * Returns a [NodePgPoolClient](https://node-postgres.com/apis/pool#poolconnect) for the given pg_env.
   *
   * @remarks
   *  This method is useful if you need to
   *
   *   * execute SQL statements within a TRANSACTION. See note-postgres
   *      [Transactions](https://node-postgres.com/features/transactions).
   *
   *   * use node-pg ETL libraries such as:
   *     * [pg-query-stream](https://github.com/brianc/node-postgres/tree/master/packages/pg-query-stream)
   *     * [pg-cursor](https://node-postgres.com/apis/cursor)
   *     * [pg-copy-streams](https://github.com/brianc/node-pg-copy-streams)
   *
   * IMPORTANT: *User MUST [release](https://node-postgres.com/apis/pool#releasing-clients) the
   *            returned client when done.*
   *
   * NOTE: If called within a TransactionContext, the returned connection is the Transaction's connection.
   *        The client.release method is disabled and handled by the TransactionContext.
   *
   * @param pg_env - The database to connect to. Optional if running in an EtlContext.
   *
   * @returns a node-pg [Client](https://node-postgres.com/apis/client)
   */
  async getDbConnection(pg_env = this.pg_env): Promise<NodePgPoolClient> {
    logger.silly(`dama_db.getDbConnection ${pg_env}`);

    if (this.isInTransactionContext) {
      const ctx = getContext();

      // TODO TODO:TODO TODO: Make sure not switching pg_env
      if (pg_env !== this.pg_env) {
        throw new Error(
          `INCONSISTENT PgEnvs: parameter pg_env=${pg_env}, ctx.meta.pgEnv=${ctx.meta.pgEnv}`
        );
      }

      logger.silly(
        // @ts-ignore
        `dama_db.getDbConnection returning connection for ${ctx.meta.__dama_db__.txn_id}`
      );

      // @ts-ignore
      return ctx.meta.__dama_db__.txn_cxn;
    }

    logger.silly(
      "dama_db.getDbConnection returning non-transaction context connection"
    );

    const db = await this.getDb(pg_env);

    const connection = await db.connect();

    return connection;
  }

  /**
   * Determine whether executing code is happening inside a TransactionContext.
   *
   * @returns true if in a TransactionContext, false otherwise.
   */
  get isInTransactionContext() {
    try {
      const ctx = getContext();

      // @ts-ignore
      if (ctx.meta.__dama_db__) {
        return true;
      }

      return false;
    } catch (err) {
      return false;
    }
  }

  /**
   * Execute all database interactions during the passed function's execution within a DB TRANSACTION.
   *
   * @remarks
   *  All database interactions that occur during the execution of the passed function happen inside
   *    a database [TRANSACTION](https://www.postgresql.org/docs/current/tutorial-transactions.html).
   *    The database TRANSACTION BEGINs when the method is called and COMMITs when the passed function returns.
   *    If the passed function throws an Error, the TRANSACTION will ROLLBACK.
   *
   * SEE: {@link DamaDb.isInTransactionContext}
   *
   * NOTE: Currently, TransactionContexts cannot be nested.
   *
   * Based on:
   *    * https://github.com/WiseLibs/better-sqlite3/blob/master/docs/api.md#transactionfunction---function
   *    * https://github.com/golergka/pg-tx
   *
   * @param fn - The function for which all database interactions will happen in the TRANSACTION.
   * @param pg_env - The database to connect to. Optional if running in an EtlContext.
   *
   * @returns the result of the passed function
   */
  async runInTransactionContext(fn: () => unknown, pg_env = this.pg_env) {
    let current_context: EtlContext;

    try {
      current_context = getContext();
    } catch (err) {
      current_context = { meta: { pgEnv: pg_env } };
    }

    //  TODO: Implement SAVEPOINTs
    //        See: https://github.com/golergka/pg-tx/blob/master/src/transaction.ts
    if (this.isInTransactionContext) {
      throw new Error("Transaction contexts cannot be nested.");
    }

    // NOTE: Not yet isInTransactionContext, therefore release not disabled.
    const db = await this.getDbConnection(pg_env);
    const txn_id = uuid();

    const txn_cxn = <NodePgPoolClient>new Proxy(
      {},
      {
        get(_target, prop) {
          if (prop === "release") {
            // eslint-disable-next-line @typescript-eslint/no-empty-function
            return () => {};
          }

          return typeof db[prop] === "function" ? db[prop].bind(db) : db[prop];
        },
      }
    );

    const meta = {
      ...current_context.meta,
      pgEnv: pg_env,
      __dama_db__: { txn_id, txn_cxn },
    };

    const txn_context = {
      ...current_context,
      meta,
    };

    try {
      await db.query("BEGIN ;");

      const result = await runInDamaContext(txn_context, fn);

      logger.silly("COMMITING", txn_id);

      await db.query("COMMIT ;");

      return result;
    } catch (err) {
      // @ts-ignore
      logger.debug((<Error>err).message);
      logger.debug((<Error>err).stack);

      try {
        logger.debug(`ROLLBACK TRANSACTION for txn_id=${txn_id}`);
        await db.query("ROLLBACK ;");
        throw err;
      } catch (err2) {
        // @ts-ignore
        logger.debug(err2.message);
        throw err2;
      }
    } finally {
      logger.silly("release txn_context", txn_id);
      db.release();
    }
  }

  /**
   * Execute the passed query/queries and return the result/results.
   *
   * @param queries - The database query or queries to execute. A query may be expressed either as a string
   *    or as a node-pg [query config object](https://node-postgres.com/features/queries#query-config-object).
   * @param pg_env - The database to connect to. Optional if running in an EtlContext.
   *
   * @returns the [result](https://node-postgres.com/apis/result)(s) of the query/queries
   */
  async query<T extends DamaDbQueryParamType>(
    queries: T,
    pg_env = this.pg_env
  ): Promise<DamaDbQueryReturnType<T>> {
    logger.silly(`dama_db.query\n${JSON.stringify(queries, null, 4)}`);

    const multi_queries = Array.isArray(queries);

    const sql_arr = multi_queries ? queries : [queries];

    const db = <NodePgPoolClient>await this.getDbConnection(pg_env);

    try {
      const results: NodePgQueryResult[] = [];

      // @ts-ignore
      for (const q of sql_arr) {
        results.push(await db.query(q));
      }

      return (multi_queries ? results : results[0]) as DamaDbQueryReturnType<T>;
    } finally {
      db.release();
    }
  }

  /**
   * Execute a SQL file.
   *
   * @param sql_file_path - Path to the SQL file.
   * @param pg_env - The database to connect to. Optional if running in an EtlContext.
   */
  async executeSqlFile(sql_file_path: string, pg_env = this.pg_env) {
    logger.silly(`dama_db.executeSqlFile ${sql_file_path}`);

    const sql = await readFileAsync(sql_file_path, { encoding: "utf8" });

    const { rows } = await this.query(sql, pg_env);

    return rows;
  }

  /**
   * Make an [AsyncGenerator](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/AsyncGenerator)
   *    over the results of a database query.
   *
   * @param query - The database query. A query may be expressed either as a string or as
   *    a node-pg [query config object](https://node-postgres.com/features/queries#query-config-object).
   * @param options - options.row_count configures how many rows to read at a time.
   *    See [Cursor.read](https://node-postgres.com/apis/cursor#read).
   * @param pg_env - The database to connect to. Optional if running in an EtlContext.
   *
   * @returns the AsyncGenerator over the query results.
   */
  async *makeIterator(
    query: string | NodePgQueryConfig,
    cursor_config: { row_count?: number } | null = null,
    pg_env = this.pg_env
  ) {
    cursor_config = cursor_config || {};

    logger.silly(
      `\ndama_db.makeIterator query=\n${JSON.stringify(query, null, 4)}`
    );

    // @ts-ignore
    const { row_count = 500 } = cursor_config;

    // @ts-ignore
    const cursorRequest = new Cursor(query);

    // TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
    // Need to make sure this works within a TransactionContext
    // Can we have a cursor open on a connection while sending queries?

    const db = <NodePgPoolClient>await this.getDbConnection(pg_env);
    const cursor = db.query(cursorRequest);

    try {
      const fn = (resolve: Function, reject: Function) => {
        cursor.read(row_count, (err, rows) => {
          if (err) {
            return reject(err);
          }

          return resolve(rows);
        });
      };

      while (true) {
        const rows: any[] = await new Promise(fn);

        if (!rows.length) {
          break;
        }

        for (const row of rows) {
          yield row;
        }
      }
    } catch (err) {
      console.error(err);
      throw err;
    } finally {
      await cursor.close();

      db.release();
    }
  }

  /**
   * Execute DDL to initialize the data_manager SCHEMA.
   *
   * @param pg_env - The database to connect to. Optional if running in an EtlContext.
   */
  async runDatabaseInitializationDDL(pg_env = this.pg_env) {
    // data_manager SCHEMA is initialized in getDb
    await this.getDb(pg_env);
  }

  /**
   * Close all database connections.
   */
  async shutdown() {
    try {
      Object.keys(this._local_.dbs).forEach(async (pg_env) => {
        try {
          const db = await this._local_.dbs[pg_env];
          if (db) {
            delete this._local_.dbs[pg_env];

            await db.end();
          }
        } catch (err) {
          //
        }
      });
    } catch (err) {
      // ignore
    }
  }
}

export default new DamaDb();
