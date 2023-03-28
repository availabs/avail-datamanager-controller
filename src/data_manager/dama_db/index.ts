import { readFile as readFileAsync } from "fs/promises";
import { join } from "path";

import _ from "lodash";

import Cursor from "pg-cursor";

import DamaContextAttachedResource from "../contexts/index";

import {
  NodePgPool,
  NodePgPoolClient,
  NodePgQueryConfig,
  NodePgQueryResult,
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

// Order matters
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

async function initializeDamaTables(dbConnection: NodePgPoolClient) {
  await dbConnection.query("BEGIN;");

  for (const scriptFile of dbInitializationScripts) {
    const sqlPath = join(__dirname, "sql", scriptFile);
    const sql = await readFileAsync(sqlPath, { encoding: "utf8" });

    await dbConnection.query(sql);
  }

  dbConnection.query("COMMIT ;");
}

// export type Query = string | NodePgQueryConfig;

class DamaDb extends DamaContextAttachedResource {
  private readonly _local_: LocalVariables;

  constructor() {
    super();

    this._local_ = <LocalVariables>{
      dbs: {},
      transactionConnections: {},
    };
  }

  async getDb(): Promise<NodePgPool> {
    if (!this._local_.dbs[this.pg_env]) {
      let resolve: Function;

      this._local_.dbs[this.pg_env] = new Promise((res) => {
        resolve = res;
      });

      const db = await getConnectedNodePgPool(this.pg_env);
      const dbConnection = await db.connect();

      try {
        await initializeDamaTables(dbConnection);

        process.nextTick(() => resolve(db));
      } catch (err) {
        // FIXME CONSIDER:  Should reject if error rather than resolving null?
        //                  That would break the idempotency check.
        this._local_.dbs[this.pg_env] = null;

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
    return this._local_.dbs[this.pg_env];
  }

  // Make sure to release the connection
  async getDbConnection() {
    const db = await this.getDb();

    const connection = await db.connect();

    return connection;
  }

  // https://node-postgres.com/features/queries#query-config-object
  async query<T extends DamaDbQueryParamType>(
    queries: T
  ): Promise<DamaDbQueryReturnType<T>> {
    const multi_queries = Array.isArray(queries);

    const sql_arr = multi_queries ? queries : [queries];

    const dbconn = <NodePgPoolClient>await this.getDbConnection();

    try {
      const results: NodePgQueryResult[] = [];

      // @ts-ignore
      for (const q of sql_arr) {
        results.push(await dbconn.query(q));
      }

      return (multi_queries ? results : results[0]) as DamaDbQueryReturnType<T>;
    } finally {
      dbconn.release();
    }
  }

  async executeSqlFile(sqlFilePath: string) {
    const sql = await readFileAsync(sqlFilePath, { encoding: "utf8" });

    const { rows } = await this.query(sql);

    return rows;
  }

  async *makeIterator(query: string | NodePgQueryConfig, config = {}) {
    // @ts-ignore
    const { rowCount = 500 } = config;

    // @ts-ignore
    const cursorRequest = new Cursor(query);

    const dbconn = <NodePgPoolClient>await this.getDbConnection();
    const cursor = dbconn.query(cursorRequest);

    try {
      const fn = (resolve: Function, reject: Function) => {
        cursor.read(rowCount, (err, rows) => {
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
      dbconn.release();
    }
  }

  async runDatabaseInitializationDDL() {
    const dbconn = <NodePgPoolClient>await this.getDbConnection();

    await initializeDamaTables(dbconn);
  }

  async shutdown() {
    try {
      Object.keys(this._local_.dbs).forEach(async (pg_env) => {
        try {
          const db = await this._local_.dbs[pg_env];

          await db?.end();
        } catch (err) {
          // ignore
        }
      });
    } catch (err) {
      // ignore
    }
  }
}

export default new DamaDb();
