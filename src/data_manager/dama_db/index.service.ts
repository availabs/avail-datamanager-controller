import { Context as MoleculerContext } from "moleculer";

import dama_db from ".";

export default {
  name: "dama_db",

  actions: {
    //  NOTE: MUST release the connection when done.
    //        See: https://node-postgres.com/api/pool#releasecallback
    getDbConnection: {
      visibility: "protected",

      handler: dama_db.getDbConnection.bind(dama_db),
    },

    runDatabaseInitializationDDL:
      dama_db.runDatabaseInitializationDDL.bind(dama_db),

    query: {
      visibility: "protected", // can be called only from local services

      // https://node-postgres.com/features/queries#query-config-object
      async handler(ctx: MoleculerContext) {
        const { params } = ctx;

        // @ts-ignore
        const { queries = params } = params;

        return dama_db.query(queries);
      },
    },

    makeIterator: {
      visibility: "protected",

      handler(ctx: MoleculerContext) {
        const {
          // @ts-ignore
          params: { query, config },
        } = ctx;

        return dama_db.makeIterator(query, config);
      },
    },

    async executeSqlFile(ctx: MoleculerContext) {
      const {
        // @ts-ignore
        params: { sqlFilePath },
      } = ctx;

      return dama_db.executeSqlFile(sqlFilePath);
    },
  },

  async stopped() {
    return dama_db.shutdown();
  },
};
