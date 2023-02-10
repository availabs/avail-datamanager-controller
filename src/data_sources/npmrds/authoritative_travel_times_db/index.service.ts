import { readFile as readFileAsync } from "fs/promises";
import { join } from "path";

import pgFormat from "pg-format";

import { Context } from "moleculer";

import { NodePgPoolClient } from "../../../data_manager/dama_db/postgres/PostgreSQL";

import { NpmrdsDataSources } from "../domain";

import makeTravelTimesExportTablesAuthoritative, {
  getEttMetadata,
} from "./actions/makeTravelTimesExportTablesAuthoritative";

export const serviceName =
  "dama/data_sources/npmrds/authoritative_travel_times_db";

const queryNpmrdsAuthoritativePartitionTreeSqlPath = join(
  __dirname,
  "./sql/queryNpmrdsAuthoritativePartitionTree.sql"
);

export default {
  name: serviceName,

  actions: {
    async getDependencyTree(ctx: Context) {
      const sql = await readFileAsync(
        queryNpmrdsAuthoritativePartitionTreeSqlPath,
        { encoding: "utf8" }
      );

      const query = pgFormat(sql, NpmrdsDataSources.NpmrdsTravelTimesExportDb);

      // @ts-ignore
      const { rows: dependencyTreeSummary } = await ctx.call(
        "dama_db.query",
        query
      );

      console.log(JSON.stringify({ dependencyTreeSummary }, null, 4));

      return dependencyTreeSummary;
    },

    makeTravelTimesExportTablesAuthoritative,

    async getEttMetadata(ctx: Context) {
      let {
        // @ts-ignore
        params: { damaViewIds },
      } = ctx;

      if (!damaViewIds) {
        throw new Error("Missing required parameter damaViewIds.");
      }

      damaViewIds = Array.isArray(damaViewIds)
        ? damaViewIds.map((vId) => +vId)
        : [+damaViewIds];

      const dbConn: NodePgPoolClient = await ctx.call(
        "dama_db.getDbConnection"
      );

      const ettMeta = await getEttMetadata(dbConn, damaViewIds);

      console.log(JSON.stringify(ettMeta, null, 4));
    },
  },
};
