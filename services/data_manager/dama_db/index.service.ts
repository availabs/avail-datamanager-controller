// @ts-ignore
import { readFile as readFileAsync } from "fs/promises";
import { join, sep } from "path";

import _ from "lodash";

import { Context } from "moleculer";

import { FSA } from "flux-standard-action";

import {
  PgEnv,
  NodePgConnection,
  getConnectedNodePgClient,
} from "../../shared/postgres/PostgreSQL";

export type ServiceContext = Context & {
  params: FSA;
};

type LocalVariables = {
  databaseConnections: Record<PgEnv, Promise<NodePgConnection>>;
};

export default {
  name: "dama_db",

  actions: {
    getDb: {
      visibility: "protected",

      async handler(ctx: Context) {
        const { params: pg_env } = ctx;

        const local = this._local_ as LocalVariables;

        if (!local.databaseConnections[pg_env]) {
          let ready: Function;

          local.databaseConnections[pg_env] = new Promise((resolve) => {
            ready = resolve;
          });

          const db = await getConnectedNodePgClient(pg_env);

          const createTablesSql = await readFileAsync(
            join(__dirname, "./sql/create_event_sourcing_table.sql"),
            { encoding: "utf8" }
          );

          await db.query(createTablesSql);

          // @ts-ignore
          ready(db);
        }

        return local.databaseConnections[pg_env];
      },
    },
  },

  created() {
    this._local_ = <LocalVariables>{
      databaseConnections: {},
    };
  },

  async stopped() {
    const local = <LocalVariables>this._local_;

    await Promise.all(
      Object.values(local.databaseConnections).map(async (dbP) => {
        try {
          const db = await dbP;
          return db.end();
        } catch (err) {
          // ignore
        }
      })
    );
  },
};
