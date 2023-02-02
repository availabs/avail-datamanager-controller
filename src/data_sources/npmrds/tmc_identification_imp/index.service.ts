import { existsSync } from "fs";
import { join, parse } from "path";

import decompress from "decompress";

import { Context } from "moleculer";

import damaHost from "../../../constants/damaHost";

import load from "./tasks/loadNpmrdsTmcIdentificationImp/main";
import computeStatistics from "./tasks/computeStatistics/main";

export const serviceName = "dama/data_sources/npmrds/tmc_identification_imp";

export default {
  name: serviceName,

  actions: {
    load: {
      visibility: "protected",
      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { npmrdsExportSqliteDbPath },
          // @ts-ignore
          meta: { pgEnv },
        } = ctx;

        if (!existsSync(npmrdsExportSqliteDbPath)) {
          throw new Error(
            `NpmrdsTravelTimesExportSqlite file ${npmrdsExportSqliteDbPath} does not exists on ${damaHost}.`
          );
        }

        const { dir, name, ext } = parse(npmrdsExportSqliteDbPath);

        // FIXME FIXME FIXME: Assumes the unzipped exists alongside the zipped.
        //                    Should unzip if necessary, then clean up.
        let npmrds_export_sqlite_db_path = npmrdsExportSqliteDbPath;

        if (ext === ".zip") {
          npmrds_export_sqlite_db_path = join(dir, name);
        }

        const unzippedExists = existsSync(npmrds_export_sqlite_db_path);

        if (!unzippedExists) {
          console.log("unzipping NpmrdsTravelTimesExportSqlite");

          await decompress(npmrdsExportSqliteDbPath, dir);
        }

        const doneData = await load({
          npmrds_export_sqlite_db_path,
          pgEnv,
        });

        return doneData;
      },
    },

    computeStatistics: {
      visibility: "protected",
      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { loadDoneData },
          // @ts-ignore
          meta: { pgEnv },
        } = ctx;

        const statistics = await computeStatistics({
          loadDoneData,
          pgEnv,
        });

        return statistics;
      },
    },
  },
};
