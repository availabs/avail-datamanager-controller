import { readFile as readFileAsync } from "fs/promises";
import { join } from "path";

import pgFormat from "pg-format";

import { Context } from "moleculer";

import dama_db from "data_manager/dama_db";

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
    async getDependencyTree() {
      const sql = await readFileAsync(
        queryNpmrdsAuthoritativePartitionTreeSqlPath,
        { encoding: "utf8" }
      );

      const query = pgFormat(sql, NpmrdsDataSources.NpmrdsTravelTimesImports);

      // @ts-ignore
      const { rows: dependencyTreeSummary } = await dama_db.query(query);

      console.log(JSON.stringify({ dependencyTreeSummary }, null, 4));

      return dependencyTreeSummary;
    },

    async makeTravelTimesExportTablesAuthoritative(ctx: Context) {
      let {
        // @ts-ignore
        params: { dama_view_ids },
      } = ctx;

      dama_view_ids = Array.isArray(dama_view_ids)
        ? dama_view_ids.map((id) => +id)
        : [+dama_view_ids];

      const done_data = await makeTravelTimesExportTablesAuthoritative(
        dama_view_ids
      );

      return done_data;
    },
  },
};
