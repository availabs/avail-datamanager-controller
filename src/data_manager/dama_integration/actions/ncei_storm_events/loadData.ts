import { Context } from "moleculer";
import { PoolClient, QueryConfig, QueryResult } from "pg";
import {FSA} from "flux-standard-action";
import dedent from "dedent";
import pgFormat from "pg-format";
import EventTypes from "../../constants/EventTypes";
import {postProcess} from "../postUploadProcessing";
import {loadFiles, createSqls} from "./utils/upload";
import {tables} from "./utils/tables";
import fs from "fs";
import https from "https";
import {execSync} from "child_process";
import {getFiles} from "./utils/scrapper";

// mol $ call 'dama/data_source_integrator.testUploadAction' --table_name details --#pgEnv dama_dev_1

const fetchFileList = (fileName, currTable) => {
  const url = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
  console.log("fetching...", url + fileName)

  if(!fs.existsSync("data")){
    fs.mkdirSync("data")
  }

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

        resolve(execSync(`gunzip -f ${file_path}`, { encoding: 'utf-8' }));
      })
    })
  })
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

const create_view = async (etl_context_id, dbConnection, ctx, sqlLog) => {
  const events: FSA[] = await ctx.call("dama_dispatcher.queryDamaEvents", {
    etl_context_id,
  });

  const eventByType = events.reduce((acc, damaEvent: FSA) => {
    acc[damaEvent.type] = damaEvent;
    return acc;
  }, {});

  const viewMetadataSubmittedEvent =
    eventByType[EventTypes.VIEW_METADATA_SUBMITTED];

  const insertViewMetaSql = await getInsertViewMetadataSql(
    ctx,
    viewMetadataSubmittedEvent
  );

  sqlLog.push(insertViewMetaSql);
  const res = await dbConnection.query(insertViewMetaSql);

  const {
    rows: [viewMetadata],
  } = res;
  return viewMetadata;

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

  const data_table = pgFormat("%I.%I", tables[table_name].schema, `${table_name}_${view_id}`);

  const q = {
    text: updateViewMetaSql,
    values: [tables[table_name].schema, `${table_name}_${view_id}`, data_table, view_id],
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
    const data_cleaning_file_path = "src/data_manager/dama_integration/actions/ncei_storm_events/utils/";

    // download step 1
    const files = await getFiles();
    console.log("downloading",table_name);
    await [table_name]
      .reduce(async (accTable, currTable) => {
        await accTable;

        return files[currTable]
          .reduce(async (acc, curr) => {
            await acc;
            return fetchFileList(curr, currTable)
          }, Promise.resolve())
      }, Promise.resolve())

    // download step 2
    const dc_op = execSync(`python ${data_cleaning_file_path}dataCleaning.py ${table_name}`, { encoding: 'utf-8' });

    console.log('dc op', dc_op);


    // insert into views, get view id, and use it in table name.

    sqlLog.push("BEGIN ;");
    res = await dbConnection.query("BEGIN ;");
    resLog.push(res);

    // create schema
    const createSchema = `CREATE SCHEMA IF NOT EXISTS ${tables[table_name].schema};`;
    sqlLog.push(createSchema);
    res = await ctx.call("dama_db.query", {
      text: createSchema
    });
    resLog.push(res);
    console.log("see this:", res.rows);

    // create table
    sqlLog.push(createSqls(table_name, view_id));
    res = await ctx.call("dama_db.query", {
      text: createSqls(table_name, view_id)
    });
    resLog.push(res);
    console.log("see this:", res.rows);

    await dbConnection.query("COMMIT;");


    // update view meta
    await update_view(table_name, view_id, dbConnection, sqlLog, resLog);

    await dbConnection.query("COMMIT;");


    await loadFiles(table_name, view_id, ctx);

    console.log("uploaded!");

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
