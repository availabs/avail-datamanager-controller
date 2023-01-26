import { readFile as readFileAsync } from "fs/promises";
import { join } from "path";

import _ from "lodash";

import Cursor from "pg-cursor";

import { Context } from "moleculer";

import { v4 as uuidv4 } from "uuid";
import pgFormat from "pg-format";
import dedent from "dedent";

import { FSA } from "flux-standard-action";

import getPgEnvFromCtx from "../dama_utils/getPgEnvFromContext";

import generateToposortedLoadDataSourcesQueries, {
  ToposortedLoadDataSourcesQueries,
} from "./actions/generateToposortedLoadDataSourcesQueries";

import {
  NodePgPool,
  NodePgPoolClient,
  NodePgQueryConfig,
  NodePgQueryResult,
  getConnectedNodePgPool,
  getPsqlCredentials,
} from "./postgres/PostgreSQL";

export type ServiceContext = Context & {
  params: FSA;
};

export type QueryLogEntry = {
  query: NodePgQueryConfig | string;
  result: NodePgQueryResult | string | null;
};

export type DamaDbTransaction = {
  transactionId: string;
  begin: () => Promise<void>;
  commit: () => Promise<void>;
  rollback: () => Promise<void>;
  queryLog: QueryLogEntry[];
};

type LocalVariables = {
  // Promise because below we only want to getDb once and this._local_.db is our "once" check.
  dbs: Record<string, Promise<NodePgPool>>;
};

// Order matters
const dbInitializationScripts = [
  "create_required_extensions.sql",
  "create_dama_core_tables.sql",
  "create_dama_etl_tables.sql",
  "create_dama_admin_helper_functions.sql",
  "create_geojson_schema_table.sql",
  "create_dama_table_schema_utils.sql",
  "create_data_source_metadata_utils.sql",
  "create_mbtiles_tables.sql",
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

export type Query = string | NodePgQueryConfig;

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

    getTransactionIdFromCtx(ctx: Context) {
      const {
        // @ts-ignore
        meta: { transactionId = null },
      } = ctx;

      return transactionId;
    },

    async *makeIterator(
      pgEnv: string,
      query: string | NodePgQueryConfig,
      config = {}
    ) {
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
    getPostgresEnvironmentVariables: {
      visibility: "protected",

      async handler(ctx: Context) {
        const pgEnv = getPgEnvFromCtx(ctx);
        const env = getPsqlCredentials(pgEnv);

        return env;
      },
    },

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

    async runDatabaseInitializationDDL(ctx: Context) {
      const pgEnv = getPgEnvFromCtx(ctx);

      const dbconn = <NodePgPoolClient>await this.getDbConnection(pgEnv);

      await initializeDamaTables(dbconn);
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
        const transactionId = this.getTransactionIdFromCtx(ctx);

        // @ts-ignore
        params = params.queries || params;
        const multiQueries = Array.isArray(params);
        const queries = multiQueries ? params : [params];

        const transactionConn = <NodePgPoolClient>(
          this._local_.transactionConnections?.[pgEnv]?.[transactionId]
        );

        if (transactionConn) {
          console.log(
            "using transaction",
            transactionId,
            "database connection"
          );
        }

        const dbconn =
          transactionConn ||
          <NodePgPoolClient>await this.getDbConnection(pgEnv);

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
          if (!transactionConn) {
            dbconn.release();
          }
        }
      },
    },

    // TODO: Deprecate this. Too complicated and error prone. Just pass query an array.
    createTransaction: {
      visibility: "protected",

      async handler(ctx: Context): Promise<DamaDbTransaction> {
        const pgEnv = getPgEnvFromCtx(ctx);
        const transactionId = uuidv4();

        this._local_.transactionConnections[pgEnv] =
          this._local_.transactionConnections[pgEnv] = {};

        this._local_.transactionConnections[pgEnv][transactionId] = {
          async query() {
            // Because DB connections are a limited resource.
            throw new Error(
              "Database connection must be created using the transaction's begin method."
            );
          },
        };

        const queryLog: QueryLogEntry[] = [];

        const cleanup = async (cmd: "COMMIT" | "ROLLBACK") => {
          try {
            await this._local_.transactionConnections[pgEnv][
              transactionId
            ].query(cmd);
          } finally {
            await this._local_.transactionConnections[pgEnv][
              transactionId
            ].release();

            delete this._local_.transactionConnections[pgEnv][transactionId];
          }
        };

        const begin = async () => {
          const dbConnection = <NodePgPoolClient>(
            await this.getDbConnection(pgEnv)
          );

          // FIXME: Need to make a more complete proxy.
          // https://github.com/DefinitelyTyped/DefinitelyTyped/blob/aa6077f5a368664068ba1f613c624fa31a5fedcc/types/pg/index.d.ts#L211-L278
          const proxy = {
            async query(query: NodePgQueryConfig | string) {
              try {
                // @ts-ignore
                const result = await dbConnection.query(query);

                queryLog.push({
                  query,
                  result: _.omit(result, "_types"),
                });

                return result;
              } catch (err) {
                queryLog.push({
                  query,
                  result: err.message,
                });
                throw err;
              }
            },

            release() {
              dbConnection.release();
            },
          };

          this._local_.transactionConnections[pgEnv][transactionId] = proxy;

          await proxy.query("BEGIN");
        };

        const commit = async () => {
          await cleanup("COMMIT");
        };

        const rollback = async () => {
          await cleanup("ROLLBACK");
        };

        return { transactionId, begin, commit, rollback, queryLog };
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

    async insertNewRow(ctx: Context) {
      const q = await this.actions.generateInsertStatement(ctx.params, {
        parentCtx: ctx,
      });
      return await this.actions.query(q, { parentCtx: ctx });
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

    // ASSUMES the following CONVENTION/INVARIANTs:
    //    1. Event type prefixed by serviceName. E.G:  `${serviceName}/foo/bar:BAZ`
    //    2. All ETL processes for the service end with a ":FINAL" or ":ERROR" event
    //    3. All status update types end with "UPDATE"
    async queryOpenEtlProcessesStatusUpdatesForService(ctx: Context) {
      const {
        // @ts-ignore
        params: { serviceName },
      } = ctx;

      const q = dedent(
        pgFormat(
          `
            SELECT
                etl_context_id,
                payload
              FROM (
                SELECT
                    etl_context_id,
                    payload,
                    row_number() OVER (PARTITION BY etl_context_id ORDER BY event_id DESC) AS row_number
                  FROM _data_manager_admin.dama_event_store
                    INNER JOIN (

                      SELECT
                          etl_context_id
                        FROM _data_manager_admin.dama_event_store
                        WHERE ( type LIKE ( %L || '%' ) )
                      EXCEPT
                      SELECT
                          etl_context_id
                        FROM _data_manager_admin.dama_event_store
                        WHERE (
                          ( type LIKE ( %L || '%' ) )
                          AND
                          (
                            ( right(type, 6) = ':FINAL' )
                            OR
                            ( right(type, 6) = ':ERROR' )
                          )
                        )

                     ) AS t USING (etl_context_id)
                  WHERE ( right(type, 6) = 'UPDATE' )
              ) AS t
              WHERE ( row_number = 1 )
          `,
          serviceName,
          serviceName
        )
      );

      // @ts-ignore
      const { rows } = await ctx.call("dama_db.query", q);

      return rows;
    },

    // ASSUMES the following CONVENTION/INVARIANT:
    //    1. All ETL processes for the service end with a single ":FINAL" or ":ERROR" event
    async queryEtlContextFinalEvent(ctx: Context) {
      const {
        // @ts-ignore
        params: { etlContextId },
      } = ctx;

      const q = dedent(
        `
          SELECT
              *
            FROM _data_manager_admin.dama_event_store
            WHERE (
              ( etl_context_id = $1 )
              AND
              (
                ( right(type, 6) = ':FINAL' )
                OR
                ( right(type, 6) = ':ERROR' )
              )
            )
        `
      );

      // @ts-ignore
      const { rows } = await ctx.call("dama_db.query", {
        text: q,
        values: [etlContextId],
      });

      if (rows.length > 1) {
        throw new Error(
          "INVARIANT VIOLATION: There MUST be a single :FINAL or :ERROR event for an EtlContext."
        );
      }

      return rows[0];
    },

    async executeSqlFile(ctx: Context) {
      const {
        // @ts-ignore
        params: { sqlFilePath },
      } = ctx;

      const sql = await readFileAsync(sqlFilePath, { encoding: "utf8" });

      // @ts-ignore
      const { rows } = await ctx.call("dama_db.query", sql);

      return rows;
    },

    generateToposortedLoadDataSourcesQueries: {
      visibility: "public",
      handler: generateToposortedLoadDataSourcesQueries,
    },

    async loadToposortedDamaSourceMetadata(ctx: Context) {
      const pgEnv = getPgEnvFromCtx(ctx);

      const opts = { parentCtx: ctx };

      const toposortedLoadDataSourcesQueries: ToposortedLoadDataSourcesQueries =
        await this.actions.generateToposortedLoadDataSourcesQueries(
          ctx.params,
          opts
        );

      const dbConnection: NodePgPoolClient = await this.getDbConnection(pgEnv);

      await dbConnection.query("BEGIN ;");

      const toposortedDamaSrcNames: string[] = [];
      try {
        for (const {
          name,
          existsQuery,
          insertQuery,
          allSourceDependencyNames,
          existingSourceDependencyNamesQuery,
          updateSourceDependenciesQuery,
        } of toposortedLoadDataSourcesQueries) {
          toposortedDamaSrcNames.push(name);

          const {
            rows: [{ data_source_exists }],
          } = await dbConnection.query(existsQuery);

          if (data_source_exists) {
            continue;
          }

          await dbConnection.query(insertQuery);

          if (!allSourceDependencyNames) {
            continue;
          }

          const {
            rows: [{ existing_source_dependency_names }],
          } = await dbConnection.query(
            <NodePgQueryConfig>existingSourceDependencyNamesQuery
          );

          const missingSrcs = _.difference(
            allSourceDependencyNames,
            existing_source_dependency_names
          );

          if (missingSrcs.length) {
            throw new Error(
              `ERROR: The following source_dependencies for ${name} do not exist: ${missingSrcs}`
            );
          }

          await dbConnection.query(
            <NodePgQueryConfig>updateSourceDependenciesQuery
          );
        }

        const damaSrcMetaText = dedent(`
          SELECT
              *
            FROM data_manager.sources
            WHERE ( name = ANY( $1 ) )
        `);

        const damaSrcMetaValues = [toposortedDamaSrcNames];

        const { rows: damaSrcMetaRows } = await dbConnection.query({
          text: damaSrcMetaText,
          values: damaSrcMetaValues,
        });

        await dbConnection.query("COMMIT ;");

        const damaSrcMetaByName = damaSrcMetaRows.reduce((acc, row) => {
          const { name } = row;
          acc[name] = row;
          return acc;
        }, {});

        const toposortedDamaSrcMeta = toposortedDamaSrcNames.map(
          (name) => damaSrcMetaByName[name]
        );

        return toposortedDamaSrcMeta;
      } catch (err) {
        await dbConnection.query("ROLLBACK ;");
        throw err;
      } finally {
        dbConnection.release();
      }
    },
  },

  created() {
    this._local_ = <LocalVariables>{
      dbs: {},
      transactionConnections: {},
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
