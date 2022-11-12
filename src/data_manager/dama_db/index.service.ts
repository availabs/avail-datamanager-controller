import { readFile as readFileAsync } from "fs/promises";
import { join } from "path";

import _ from "lodash";

import Cursor from "pg-cursor";

import { Context } from "moleculer";

import pgFormat from "pg-format";
import dedent from "dedent";

import { FSA } from "flux-standard-action";

import getPgEnvFromCtx from "../dama_utils/getPgEnvFromContext";

import {
  NodePgPool,
  NodePgPoolClient,
  getConnectedNodePgPool,
} from "./postgres/PostgreSQL";

export type ServiceContext = Context & {
  params: FSA;
};

type LocalVariables = {
  // Promise because below we only want to getDb once and this._local_.db is our "once" check.
  dbs: Record<string, Promise<NodePgPool>>;
};

// Order matters
const dbInitializationScripts = [
  "create_dama_core_tables.sql",
  "create_dama_etl_tables.sql",
  "create_dama_admin_helper_functions.sql",
  "create_geojson_schema_table.sql",
  "create_dama_table_schema_utils.sql",
  "create_data_source_metadata_utils.sql",
];

async function initializeDamaTables(dbConnection: NodePgPoolClient) {
  await dbConnection.query("BEGIN;");

  for (const scriptFile of dbInitializationScripts) {
    const sqlPath = join(__dirname, "sql", scriptFile);
    const sql = await readFileAsync(sqlPath, { encoding: "utf8" });

    await dbConnection.query(sql);
  }

  dbConnection.query("COMMIT ;");
}

export default {
  name: "dama_db",

  methods: {
    async getDb(pgEnv: string) {
      if (!this._local_.dbs[pgEnv]) {
        let resolve: Function;

        this._local_.dbs[pgEnv] = new Promise((res) => {
          resolve = res;
        });

        const db = await getConnectedNodePgPool(pgEnv);
        const dbConnection = await db.connect();

        try {
          await initializeDamaTables(dbConnection);

          dbConnection.release();

          // @ts-ignore
          resolve(db);
        } catch (err) {
          this._local_.dbs[pgEnv] = null;

          console.error(err);
          db.end();
          // @ts-ignore
          resolve(null);
        }
      }

      return this._local_.dbs[pgEnv];
    },

    // Make sure to release the connection
    async getDbConnection(pgEnv: string) {
      const db = await this.getDb(pgEnv);

      const connection = await db.connect();

      return connection;
    },

    async *makeIterator(pgEnv, query, config = {}) {
      // @ts-ignore
      const { rowCount = 500 } = config;

      // @ts-ignore
      const cursorRequest = new Cursor(query);

      const dbconn = <NodePgPoolClient>await this.getDbConnection(pgEnv);
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
    },
  },

  actions: {
    getDb: {
      visibility: "protected", // can be called only from local services

      handler(ctx: Context) {
        const pgEnv = getPgEnvFromCtx(ctx);

        return this.getDb(pgEnv);
      },
    },

    // https://node-postgres.com/api/pool#releasecallback
    // > The releaseCallback releases an acquired client back to the pool.
    // MUST release the connection when done.
    getDbConnection: {
      visibility: "protected",

      async handler(ctx: Context) {
        const pgEnv = getPgEnvFromCtx(ctx);

        return await this.getDbConnection(pgEnv);
      },
    },

    //  Execute a query or array of queries.
    //    Works with
    //      * SQL strings
    //      * node-postgres query config objects
    //        see: https://node-postgres.com/features/queries#query-config-object
    query: {
      visibility: "protected", // can be called only from local services

      // https://node-postgres.com/features/queries#query-config-object
      async handler(ctx: Context) {
        let {
          // @ts-ignore
          params,
        } = ctx;
        const pgEnv = getPgEnvFromCtx(ctx);

        // @ts-ignore
        params = params.queries || params;
        const multiQueries = Array.isArray(params);
        const queries = multiQueries ? params : [params];

        const dbconn = <NodePgPoolClient>await this.getDbConnection(pgEnv);

        try {
          const results = [];

          // @ts-ignore
          for (const q of queries) {
            // console.log(JSON.stringify(q, null, 4));
            // @ts-ignore
            results.push(await dbconn.query(q));
          }

          return multiQueries ? results : results[0];
        } finally {
          dbconn.release();
        }
      },
    },

    async generateInsertStatement(ctx: Context) {
      const {
        params: {
          // @ts-ignore
          tableSchema,
          // @ts-ignore
          tableName,
          // @ts-ignore
          newRow,
        },
      } = ctx;

      const newRowProps = Object.keys(newRow);

      // Get the data_manager.sources table columns
      const tableDescription = await this.actions.describeTable(
        {
          tableSchema,
          tableName,
        },
        { parentCtx: ctx }
      );

      const tableCols = Object.keys(tableDescription);

      const createStmtCols = _.intersection(newRowProps, tableCols);

      const insrtStmtObj = createStmtCols.reduce(
        (acc, col, i) => {
          acc.formatTypes.push("%I");
          acc.formatValues.push(col);

          const { column_type } = tableDescription[col];

          acc.placeholders.push(`$${i + 1}::${column_type}`);

          const v = newRow[col];
          acc.values.push(v === "" ? null : v);

          return acc;
        },
        {
          formatTypes: <string[]>[],
          formatValues: <string[]>[],
          placeholders: <string[]>[],
          values: <any[]>[],
        }
      );

      const text = dedent(
        pgFormat(
          `
            INSERT INTO %I.%I (
                ${insrtStmtObj.formatTypes}
              ) VALUES (${insrtStmtObj.placeholders})
              RETURNING *
            ;
          `,
          tableSchema,
          tableName,
          ...insrtStmtObj.formatValues
        )
      );

      const { values } = insrtStmtObj;

      return { text, values };
    },

    async describeTable(ctx: Context) {
      // @ts-ignore
      const { tableSchema, tableName } = ctx.params;

      const text = dedent(`
        SELECT
            column_name,
            column_type,
            column_number
          FROM _data_manager_admin.table_column_types
          WHERE (
            ( table_schema = $1 )
            AND
            ( table_name = $2 )
          )
        ;
      `);

      const { rows } = await ctx.call("dama_db.query", {
        text,
        values: [tableSchema, tableName],
      });

      if (rows.length === 0) {
        return null;
      }

      const tableDescription = rows.reduce(
        (acc: any, { column_name, column_type, column_number }) => {
          acc[column_name] = { column_type, column_number };
          return acc;
        },
        {}
      );

      return tableDescription;
    },

    makeIterator: {
      visibility: "protected",

      handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { query, config },
        } = ctx;

        const pgEnv = getPgEnvFromCtx(ctx);

        return this.makeIterator(pgEnv, query, config);
      },
    },
  },

  created() {
    this._local_ = <LocalVariables>{
      dbs: {},
    };
  },

  async stopped() {
    try {
      Object.keys(this._local_.dbs).forEach(async (pgEnv) => {
        try {
          await this._local_.dbs[pgEnv].end();
        } catch (err) {
          // ignore
        }
      });
    } catch (err) {
      // ignore
    }
  },
};
