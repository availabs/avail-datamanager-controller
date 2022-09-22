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

  methods: {
    async initializeDamaTables(db?: NodePgConnection) {
      if (!db) {
        return this.getDb();
      }

      const initializeDamaSchemasSql = await readFileAsync(
        join(__dirname, "./sql/data_manager_schema.sql"),
        { encoding: "utf8" }
      );

      //  If the core dama tables already exist, this will throw.
      try {
        // @ts-ignore
        await (<Promise>db.query("BEGIN"));
        // @ts-ignore
        await (<Promise>db.query(initializeDamaSchemasSql));
        // @ts-ignore
        await (<Promise>db.query("COMMIT"));
      } catch (err) {
        // Ignore Error: https://stackoverflow.com/a/4480393/3970755
        // @ts-ignore
        await (<Promise>db.query("ROLLBACK"));
        // console.error(err);
      }

      const createTablesSql = await readFileAsync(
        join(__dirname, "./sql/create_event_sourcing_table.sql"),
        { encoding: "utf8" }
      );

      // @ts-ignore
      await (<Promise>db.query(createTablesSql));
    },

    async getDb() {
      if (!this._local_.db) {
        console.log("$@".repeat(50));
        let ready: Function;

        this._local_.db = new Promise((resolve) => {
          ready = resolve;
        });

        const db = await getConnectedNodePgClient();

        await this.initializeDamaTables(db);

        // @ts-ignore
        ready(db);
      }

      return this._local_.db;
    },
  },

  actions: {
    initializeDamaTables: {
      visibility: "public",

      handler() {
        return this.initializeDamaTables();
      },
    },

    getDb: {
      visibility: "protected", // can be called only from local services

      handler() {
        return this.getDb();
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
