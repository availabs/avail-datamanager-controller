import { Context } from "moleculer";
import { PoolClient, QueryConfig, QueryResult } from "pg";
import {FSA} from "flux-standard-action";
import dedent from "dedent";
import pgFormat from "pg-format";
import EventTypes from "../../constants/EventTypes";
import {postProcess} from "../postUploadProcessing";
import {tables} from "./tables";
import fs from "fs";
import https from "https";
import {execSync} from "child_process";
import {loadFiles} from "./upload";


// mol $ call 'dama/data_source_integrator.testUploadAction' --table_name details --#pgEnv dama_dev_1

const fetchFileList = async (currTable, from, to) => {
  console.log("fetching...");

  if(!fs.existsSync("data/" + currTable)){
    fs.mkdirSync("data/" + currTable);
  }

  let years  = [];
  let promises = []

  for ( let year = from; year <= to; year++){ years.push(year) }

    years.forEach(async (year) => {
      console.log('year', year)

      const url = `https://www.rma.usda.gov/-/media/RMA/Cause-Of-Loss/Summary-of-Business-with-Month-of-Loss/colsom_${year}.ashx?la=en`;
      const file_path = "data/" + currTable + "/" + `${year}.zip`;
      const command = `zcat ${file_path} > ${file_path.replace('.zip', '.txt')}`
      const file = fs.createWriteStream(file_path);

      promises.push(
        new Promise((resolve) => {
          https.get(url, response => {
            response.pipe(file);
            console.log('got: ', url);
            file.on('finish', f => {
              file.close();

              file.once('close', () => {
                file.removeAllListeners();
              });

              resolve(execSync(command, {encoding: 'utf-8'}));
            });

          })
        })
      );
    });

  return Promise.all(promises).then(() => console.log('unzipped'))


  // return years.reduce(async (acc, year) => {
  //   console.log('unzipping', year)
  //   await acc;
  //   console.log('unzipping 1', year)
  //   const file_path = "data/" + currTable + "/" + `${year}.zip`;
  //   const command = `zcat ${file_path} > ${file_path.replace('.zip', '.txt')}`
  //   execSync(command, {encoding: 'utf-8'});
  //
  // }, Promise.resolve());

}

async function getInsertViewMetadataSql(
  ctx: Context,
  viewMetadataSubmittedEvent: FSA
) {
  const insertViewMetaSql = <QueryConfig>(
    await ctx.call(
      "dama/metadata.getInsertDataManagerViewMetadataSql",
      viewMetadataSubmittedEvent
    )
  );

  return insertViewMetaSql;
}

const update_view = async (table_name, view_id, dbConnection, sqlLog, resLog) => {
  const updateViewMetaSql = dedent(
    `
        UPDATE data_manager.views
          SET
            table_schema  = $1,
            table_name    = $2,
            data_table    = $3
          WHERE view_id = $4
      `
  );

  const data_table = pgFormat("%I.%I", tables[table_name](view_id).schema, `${table_name}_${view_id}`);

  const q = {
    text: updateViewMetaSql,
    values: [tables[table_name](view_id).schema, `${table_name}_${view_id}`, data_table, view_id],
  };

  sqlLog.push(q);
  const res = await dbConnection.query(q);
  resLog.push(res);
};

export default async function publish(ctx: Context) {
  // throw new Error("publish TEST ERROR");

  let {
    // @ts-ignore
    params: { etl_context_id, table_name, view_id },
  } = ctx;
  //
  if (!(etl_context_id)) {
    const etlcontextid = await ctx.call(
      "dama_dispatcher.spawnDamaContext",
      { etl_context_id: null }
    );
    etl_context_id = etlcontextid;
    throw new Error("The etl_context_id parameter is required.");
  }

  const dbConnection: PoolClient = await ctx.call("dama_db.getDbConnection");
  const sqlLog: any[] = [];
  const resLog: QueryResult[] = [];

  // let view_id = 13
  try {
    let res: QueryResult;

    // sqlLog.push("BEGIN ;");
    // res = await dbConnection.query("BEGIN ;");
    // resLog.push(res);
    //
    // create schema
    const createSchema = `CREATE SCHEMA IF NOT EXISTS open_fema_data;`;
    sqlLog.push(createSchema);
    res = await ctx.call("dama_db.query", {
      text: createSchema
    });
    resLog.push(res);
    console.log("see this:", res.rows);

    console.log("downloading",table_name);
    await fetchFileList(table_name, 1989, 2022);
    await loadFiles(ctx, view_id, dbConnection);

    await dbConnection.query("COMMIT;");
    //
    //
    // // update view meta
    await update_view(table_name, view_id, dbConnection, sqlLog, resLog);
    //
    // await dbConnection.query("COMMIT;");
    //
    //
    // await loadFiles(table_name, view_id, ctx);
    //
    // console.log("uploaded!");

    // We need the data_manager.views id
    await dbConnection.query("COMMIT;");
    dbConnection.release();

    const finalEvent = {
      type: EventTypes.FINAL,
      payload: {
        data_manager_view_id: -1,
        createSchema: sqlLog,
        createTable: sqlLog,
        publishCmdResults: resLog,
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
        successfulcreateSchema: sqlLog,
        successfulcreateTable: sqlLog,
        successfulPublishCmdResults: resLog,
      },
      meta: {
        timestamp: new Date().toISOString(),
        etl_context_id
      },
    };

    await ctx.call("dama_dispatcher.dispatch", errEvent);

    await dbConnection.query("ROLLBACK;");

    throw err;
  }
}
