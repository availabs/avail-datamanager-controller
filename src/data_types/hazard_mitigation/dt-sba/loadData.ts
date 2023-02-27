import {execSync} from "child_process";
import fs from "fs";
import { Context } from "moleculer";
import {getFiles} from "./scrapper_fail";
import {loadFiles} from "./upload";
import {err, fin, init, update_view} from "../utils/macros";

const fetchFileList = async (fileName, url, table_name) => {
  console.log("fetching...", url);
  if(!fs.existsSync(`data/sba/`)){
    fs.mkdirSync(`data/sba/`);
  }
  const file_path = "data/" + `sba/` + fileName;

  execSync(`curl -o ${file_path} '${url}'`);

  // execSync(`rm -rf data/tl_2017_${table_name}/tmp_dir`);
};

export default async function publish(ctx: Context) {
  const {
    // @ts-ignore
    params: { table_name }
  } = ctx;

  const {etl_context_id, dbConnection, source_id, view_id, sqlLog} = await init({ctx, type: table_name});

  try {
    const data_cleaning_file_path = "src/data_types/hazard_mitigation/dt-sba/dataCleaning.py";

    const files = await getFiles();

    await files.reduce(async (acc, curr, i) => {
              await acc;
              const fileName = curr.split("/")[curr.split("/").length - 1]
              return fetchFileList(fileName, curr, table_name);
            }, Promise.resolve());

    execSync(`python ${data_cleaning_file_path}`, { encoding: 'utf-8' });

    await loadFiles(ctx, view_id, table_name);

    execSync(`rm -rf data/sba`);

    await update_view({table_schema: 'open_fema_data', table_name: table_name.toLowerCase(), view_id, dbConnection, sqlLog});

    return fin({etl_context_id, ctx, dbConnection, payload: {view_id, source_id}});
  } catch (e) {
    return err({e, etl_context_id, sqlLog, ctx, dbConnection});
  }
}
