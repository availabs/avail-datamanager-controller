import { Context } from "moleculer";

import { PoolClient, QueryConfig, QueryResult } from "pg";
import {execSync} from "child_process";
import EventTypes from "../constants/EventTypes";
import fs from "fs";
import https from "https";
import {execSync} from "child_process";
import {getFiles} from "./scrapper";
import {data} from "cheerio/lib/api/attributes";

export const ReadyToPublishPrerequisites = [
  EventTypes.QA_APPROVED,
  EventTypes.VIEW_METADATA_SUBMITTED,
];

const url = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/'
// mol $ call 'dama/data_source_integrator.testDownloadAction' --table_name details --#pgEnv dama_dev_1

const fetchFileList = (fileName, currTable) => {
  console.log('fetching...', url + fileName)
  if(!fs.existsSync("data/" + currTable)){
    fs.mkdirSync("data/" + currTable)
  }

  const file_path = "data/" + currTable + "/" + fileName;
  const file = fs.createWriteStream(file_path);

  return new Promise((resolve, reject) => {
    https.get(url + fileName, response => {
      response.pipe(file);
      console.log('got: ', url + fileName)
      file.on('finish', f => {
        file.close();
        file.once('close', () => {
          file.removeAllListeners();
        });

        resolve(execSync(`gunzip ${file_path}`, { encoding: 'utf-8' }));
      })
    })
  })
}

export default async function publish(ctx: Context) {
  // throw new Error("publish TEST ERROR");

  const etlcontextid = await ctx.call(
    "dama_dispatcher.spawnDamaContext",
    { etl_context_id: null, table_name: "details" }
  );

  const {
    // @ts-ignore
    params: { etl_context_id = etlcontextid, table_name },
  } = ctx;
  //
  if (!(etl_context_id)) {
    throw new Error("The etl_context_id parameter is required.");
  }

  try {
    const data_cleaning_file_path = "src/data_manager/dama_integration/actions";

    // step 1
    const files = await getFiles();
    console.log("downloading",table_name)
    await [table_name]
      .reduce(async (accTable, currTable) => {
        await accTable;

        return files[currTable]
          .reduce(async (acc, curr) => {
            await acc;
            return fetchFileList(curr, currTable)
          }, Promise.resolve())
      }, Promise.resolve())

    // step 2
    const dc_op = execSync(`python ${data_cleaning_file_path}/dataCleaning.py ${table_name}`, { encoding: 'utf-8' });

    console.log('dc op', dc_op)

    const finalEvent = {
      type: EventTypes.FINAL,
      payload: {
        data_manager_view_id: -1,
      },
      meta: {
        timestamp: new Date().toISOString(),
        etl_context_id
      },
    };

    await ctx.call("dama_dispatcher.dispatch", finalEvent);

    return finalEvent;
  } catch (err) {
    console.error(err);

    const errEvent = {
      type: EventTypes.PUBLISH_ERROR,
      payload: {
        message: err.message,
      },
      meta: {
        timestamp: new Date().toISOString(),
        etl_context_id
      },
    };

    await ctx.call("dama_dispatcher.dispatch", errEvent);

    throw err;
  }
}
