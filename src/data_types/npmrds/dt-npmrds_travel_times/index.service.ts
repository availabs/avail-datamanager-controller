import { readFile as readFileAsync } from "fs/promises";
import { join } from "path";

import pgFormat from "pg-format";

import { Context } from "moleculer";

import { NodePgPoolClient } from "../../../data_manager/dama_db/postgres/PostgreSQL";

import { NpmrdsDataSources } from "../domain";

import makeTravelTimesExportTablesAuthoritative from "./actions/makeTravelTimesExportTablesAuthoritative";

export const serviceName = "dama/data_types/npmrds/dt-npmrds_travel_times";

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

      const query = pgFormat(sql, NpmrdsDataSources.NpmrdsTravelTimesImp);

      // @ts-ignore
      const { rows: dependencyTreeSummary } = await ctx.call(
        "dama_db.query",
        query
      );

      console.log(JSON.stringify({ dependencyTreeSummary }, null, 4));

      return dependencyTreeSummary;
    },

    makeTravelTimesExportTablesAuthoritative,
  },
};
