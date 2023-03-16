import {execSync} from "child_process";
import fs from "fs";
import {Context} from "moleculer";
import {getFiles} from "./scrapper_fail";
import {loadFiles} from "./upload";
import {err, fin, init, update_view} from "../utils/macros";
import {sql} from "./sba_update_geoid_1";
import {tables} from "./tables";

const fetchFileList = async (fileName, url, table_name) => {
  console.log("fetching...", url);
  if (!fs.existsSync(`tmp-etl/sba/`)) {
    fs.mkdirSync(`tmp-etl/sba/`);
  }
  const file_path = "tmp-etl/" + `sba/` + fileName;

  const cmd = `curl -o ${file_path} 'https://www.sba.gov${url}'`;
  console.log('cmd', cmd)
  execSync(cmd);

  // execSync(`rm -rf tmp-etl/tl_2017_${table_name}/tmp_dir`);
};

export default async function publish(ctx: Context) {
  const {
    // @ts-ignore
    params: {table_name, state_schema, state_table, county_schema, county_table}
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

    execSync(`python ${data_cleaning_file_path}`, {encoding: 'utf-8'});

    await loadFiles(ctx, view_id, table_name);
    console.log(sql({
      table_name: tables[table_name](view_id).name, table_schema: tables[table_name](view_id).schema,
      state_schema, state_table, county_schema, county_table
    }));

    await ctx.call("dama_db.query", {
      text: sql({
        table_name: tables[table_name](view_id).name, table_schema: tables[table_name](view_id).schema,
        state_schema, state_table, county_schema, county_table
      }),
    });

    execSync(`rm -rf tmp-etl/sba`);

    await update_view({
      table_schema: 'open_fema_data',
      table_name: table_name.toLowerCase(),
      view_id,
      dbConnection,
      sqlLog
    });

    return fin({etl_context_id, ctx, dbConnection, payload: {view_id, source_id}});
  } catch (e) {
    return err({e, etl_context_id, sqlLog, ctx, dbConnection});
  }
}
