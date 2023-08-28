import { execSync } from "child_process";
import { PoolClient } from "pg";
import logger from "data_manager/logger";
import dama_meta from "data_manager/meta";
import dama_db from "data_manager/dama_db";
import dama_events from "data_manager/events";

import { getFiles } from "./scrapper";
import { update_view } from "../utils/macros";
import EventTypes from "../constants/EventTypes";
import { createViewMbtiles } from "../../dt-gis_dataset/mbtiles/mbtiles";
import {
  createView,
  createIndices,
  fetchFileList,
  uploadFiles,
  mergeTables,
  dropTmpTables,
  correctGeoid,
} from "./actions";
import getEtlWorkDir from "var/getEtlWorkDir";

export default async function publish({
  pgEnv,
  user_id,
  source_id,
  table_name,
  source_name,
  etl_context_id: etlContextId,
  existing_view_id,
  isNewSourceCreate,
  view_dependencies = "{}",
  customViewAttributes = {},
}) {
  logger.info(
   `inside publish first,
    etlContextId: ${etlContextId},
    userId: ${user_id},
    source_id: ${source_id},
    customViewAttributes: ${JSON.stringify(customViewAttributes, null, 3)},
    isNewSourceCreate: ${isNewSourceCreate},
  `
  );

  let damaSource: Record<
    string,
    string | number | Record<string, any> | Array<string | number>
  > | null = null;

  if (!source_id) {
    logger.info("Reached here inside publish source create");
    damaSource = await dama_meta.createNewDamaSource({
      name: source_name,
      type: `tl_${table_name?.toLowerCase()}`,
    });
    logger.info(`New Source Created:  ${JSON.stringify(damaSource, null, 3)}`);
    source_id = damaSource?.source_id;
  }

  const url = `https://www2.census.gov/geo/tiger/TIGER2017/${table_name}/`;

  const sqlLog: Array<any> = [];
  const dbConnection: PoolClient = await dama_db.getDbConnection();

  logger.info("is new schema geo Created?");
  await dbConnection.query("CREATE SCHEMA IF NOT EXISTS geo");
  logger.info("Yayyyy! geo Created/updated?");

  logger.info("About to create new view");
  let view_id = parseInt(existing_view_id, 10);
  let newDamaView: any = null;
  if (isNaN(view_id)) {
    logger.info("is View Created?");
    newDamaView = await createView(
      {
        source_id,
        user_id: 7,
        view_dependencies: JSON.parse(view_dependencies),
        metadata: { ...customViewAttributes },
        table_name: `tl_2017_${table_name?.toLowerCase()}`,
      },
      dbConnection
    );
    logger.info("Yayy! View Created");
    if (newDamaView && newDamaView?.view_id) {
      logger.info("View_id assigned");
      view_id = newDamaView.view_id;
    }
  }

  await dbConnection.query("BEGIN ;");
  sqlLog.push("BEGIN ;");

  try {
    const tmpLocation = getEtlWorkDir();

    logger.info(`\nGet into new try block: ${tmpLocation}`);
    const files = await getFiles(url);

    logger.info(`\nNew Files Array: ${JSON.stringify(files)}`);

    logger.info("\nreached here ----- 1 -----");
    const uploadFileEvent = {
      type: "Tiger_dataset:GIS_FILE_UPLOAD_PROGRESS",
      payload: {
        files,
      },
      meta: {
        user_id,
        timestamp: new Date().toISOString(),
      },
    };
    logger.info(`\nUpload Event ${JSON.stringify(uploadFileEvent, null, 3)}`);
    await dama_events.dispatch(uploadFileEvent, etlContextId);

    await files?.reduce(async (acc, curr) => {
      await acc;
      return fetchFileList(curr, url, tmpLocation).then(() =>
        uploadFiles(curr, pgEnv, url, table_name, view_id, tmpLocation)
      );
    }, Promise.resolve());

    execSync(`rm -rf ${tmpLocation}`);

    logger.info(
      `new damaview ${newDamaView.table_name} and table_name : ${table_name}`
    );

    logger.info("\nreached here ----- 2 -----");
    if (files?.length > 1) {
      logger.info("\nreached here ----- 3 -----");

      const mergeTableEvent = {
        type: "Tiger_dataset:MERGE_TABLE_EVENT",
        payload: {
          files,
        },
        meta: {
          user_id,
          view_id,
          table_name,
          timestamp: new Date().toISOString(),
        },
      };
      logger.info(
        `\nMerge Table Event ${JSON.stringify(mergeTableEvent, null, 3)} `
      );
      await dama_events.dispatch(mergeTableEvent, etlContextId);

      await mergeTables(
        files?.map((f) => f?.replace(".zip", "")),
        view_id,
        table_name,
        dbConnection
      );

      logger.info(
        `\nAdding Primary Key using: \n
        ALTER TABLE geo.tl_2017_${table_name}_${view_id} ADD COLUMN ogc_fid SERIAL PRIMARY KEY;`
      );
      await dbConnection.query(
        `ALTER TABLE geo.tl_2017_${table_name}_${view_id} ADD COLUMN ogc_fid SERIAL PRIMARY KEY;`
      );

      logger.info("\nreached here ----- 4 -----");

      const dropTableEvent = {
        type: "Tiger_dataset:DROP_TABLE_EVENT",
        payload: {
          files,
        },
        meta: {
          user_id,
          timestamp: new Date().toISOString(),
        },
      };
      logger.info(
        `\nDrop Table Event ${JSON.stringify(dropTableEvent, null, 3)} `
      );
      await dama_events.dispatch(dropTableEvent, etlContextId);

      await dropTmpTables(
        files?.map((f: string) => f?.replace(".zip", "")),
        dbConnection
      );
    }

    logger.info("\nreached here ----- 5 -----");

    const createIndiceEvent = {
      type: "Tiger_dataset:CREATE_INDICES_EVENT",
      payload: {
        files,
      },
      meta: {
        user_id,
        timestamp: new Date().toISOString(),
      },
    };
    logger.info(
      `\nCreate Indices Event ${JSON.stringify(createIndiceEvent, null, 3)} `
    );
    await dama_events.dispatch(createIndiceEvent, etlContextId);

    await createIndices(
      view_id,
      table_name?.toLowerCase(),
      sqlLog,
      dbConnection
    );

    logger.info("\nreached here ----- 6 -----");
    const correctGeoidsEvent = {
      type: "Tiger_dataset:CORRECT_GEOIDS_EVENT",
      payload: {
        files,
      },
      meta: {
        user_id,
        timestamp: new Date().toISOString(),
      },
    };
    logger.info(
      `\ncorrectGeoidsEvent Event ${JSON.stringify(
        correctGeoidsEvent,
        null,
        3
      )} `
    );
    await dama_events.dispatch(correctGeoidsEvent, etlContextId);

    await correctGeoid(
      view_id,
      table_name?.toLowerCase(),
      sqlLog,
      dbConnection
    );

    logger.info("\nreached here ----- 7 -----");

    await update_view({
      table_schema: "geo",
      table_name: `tl_2017_${table_name.toLowerCase()}`,
      view_id,
      dbConnection,
    });
    logger.info("\nreached here ----- 8 -----");

    await dbConnection.query("COMMIT;");

    if (isNewSourceCreate) {
      logger.info("called inside setSourceMetadata");
      dbConnection.query({
        text: "CALL _data_manager_admin.initialize_dama_src_metadata_using_view( $1 )",
        values: [view_id],
      });
    }

    // Create Mbtile
    await createViewMbtiles(view_id, source_id, etlContextId, {
      preserveColumns: ["geoid"],
    });

    const finalEvent = {
      type: "Tiger_dataset.FINAL",
      payload: {
        damaSourceId: source_id,
        damaViewId: view_id,
      },
      meta: {
        user_id,
        timestamp: new Date().toISOString(),
      },
    };

    logger.info(`\nFinal Event ${JSON.stringify(finalEvent, null, 3)} `);
    await dama_events.dispatch(finalEvent, etlContextId);

    return finalEvent;
  } catch (e) {
    logger.info("\nreached here ----- 10: Error -----");

    await dbConnection.query("ROLLBACK;");

    const errEvent = {
      type: EventTypes.PUBLISH_ERROR,
      payload: {
        message: (e as any).message,
        successfulcreateSchema: sqlLog,
        successfulcreateTable: sqlLog,
      },
      meta: {
        timestamp: new Date().toISOString(),
        etl_context_id: etlContextId,
      },
    };
    await dama_events.dispatch(errEvent, etlContextId);
    throw e;
  } finally {
    dbConnection.release();
  }
}
