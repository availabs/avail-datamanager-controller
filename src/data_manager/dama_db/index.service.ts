/*
    TODO:
          1. Use Pool: https://node-postgres.com/api/pool
 */

// @ts-ignore
import { readFile as readFileAsync } from "fs/promises";
import { join } from "path";

import _ from "lodash";

import { Context } from "moleculer";

import { FSA } from "flux-standard-action";

import {
  NodePgConnection,
  getConnectedNodePgClient,
} from "./postgres/PostgreSQL";

export type ServiceContext = Context & {
  params: FSA;
};

type LocalVariables = {
  // Promise because below we only want to getDb once and this._local_.db is our "once" check.
  db: Promise<NodePgConnection>;
};

export default {
  name: "dama_db",

  actions: {
    getDb: {
      visibility: "protected",

      async handler(): Promise<NodePgConnection> {
        if (!this._local_.db) {
          let ready: Function;

          this._local_.db = new Promise((resolve) => {
            ready = resolve;
          });

          const db = await getConnectedNodePgClient();

          const createTablesSql = await readFileAsync(
            join(__dirname, "./sql/create_event_sourcing_table.sql"),
            { encoding: "utf8" }
          );

          await db.query(createTablesSql);

          // @ts-ignore
          ready(db);
        }

        return this._local_.db;
      },
    },
  },

  created() {
    this._local_ = <LocalVariables>{};
  },

  async stopped() {
    try {
      this._local_.db.end();
    } catch (err) {
      // ignore
    }
  },
};
