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
  createViewTable,
} from "./actions";
import getEtlWorkDir from "var/getEtlWorkDir";

export default async function publish({
  pgEnv,
  user_id,
  source_id,
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
      type: "tl_full",
    });
    logger.info(`New Source Created:  ${JSON.stringify(damaSource, null, 3)}`);
    source_id = damaSource?.source_id;
  }

  let dbConnection: PoolClient = await dama_db.getDbConnection();
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
      },
      dbConnection
    );
    logger.info("Yayy! View Created");
    if (newDamaView && newDamaView?.view_id) {
      logger.info("View_id assigned");
      view_id = newDamaView.view_id;
    }
  }

  logger.info("is new schema geo Created?");
  try {
    await dbConnection.query("CREATE SCHEMA IF NOT EXISTS temp");
    await dbConnection.query("CREATE SCHEMA IF NOT EXISTS tiger");
  } catch (error) {
    logger.info("Error in the create schemas");
  }

  const roundOff10 = (n: number) => n - (n % 10);
  logger.info("Yayyyy! geo Created/updated?");
  const finalTableQueries: Array<string> = [];
  const tempTableNames: Array<string> = [];
  const domain = ["STATE", "COUNTY", "TRACT", "COUSUB"];
  const years = [2017, 2020];
  for (const [, tigerYear] of years.entries()) {
    for (const [index, table_name] of domain.entries()) {
      const year: number = roundOff10(tigerYear);
      logger.info("\n\n------------- For the new Table --------------------");
      logger.info(
        `\n\n------------- ${table_name} ------------------------\n\n`
      );
      const url = `https://www2.census.gov/geo/tiger/TIGER${tigerYear}/${table_name}/`;

      const sqlLog: Array<any> = [];
      dbConnection = await dama_db.getDbConnection();

      await dbConnection.query("BEGIN ;");
      sqlLog.push("BEGIN ;");

      try {
        const tmpLocation = `${getEtlWorkDir()}_v${view_id}_y${roundOff10(
          year
        )}_${index}`;

        logger.info(`\nGet into new try block: ${tmpLocation}`);
        const files = await getFiles(url);

        // --------------First drop the tables if exist ----------------------
        await dropTmpTables(
          files?.map((f: string) => f?.replace(".zip", "")),
          dbConnection
        );
        // --------------------------------------------------------------------

        logger.info(`\nNew Files Array: ${JSON.stringify(files)}`);

        logger.info("\nreached here ----- 1 -----");
        const uploadFileEvent = {
          type: `Tiger_dataset:${table_name}_GIS_FILE_UPLOAD_PROGRESS`,
          payload: {
            files,
          },
          meta: {
            user_id,
            timestamp: new Date().toISOString(),
          },
        };
        logger.info(
          `\nUpload Event ${JSON.stringify(uploadFileEvent, null, 3)}`
        );
        await dama_events.dispatch(uploadFileEvent, etlContextId);

        await files?.reduce(async (acc, curr) => {
          await acc;
          return fetchFileList(curr, url, tmpLocation).then(() =>
            uploadFiles(
              curr,
              pgEnv,
              url,
              table_name,
              source_id,
              view_id,
              year,
              tmpLocation
            )
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
            type: `Tiger_dataset:${table_name}_MERGE_TABLE_EVENT`,
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
            source_id,
            view_id,
            year,
            table_name,
            dbConnection
          );

          logger.info("\nreached here ----- 4 -----");

          const dropTableEvent = {
            type: `Tiger_dataset:${table_name}_DROP_TABLE_EVENT`,
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
          type: `Tiger_dataset:${table_name}_CREATE_INDICES_EVENT`,
          payload: {
            files,
          },
          meta: {
            user_id,
            timestamp: new Date().toISOString(),
          },
        };
        logger.info(
          `\nCreate Indices Event ${JSON.stringify(
            createIndiceEvent,
            null,
            3
          )} `
        );
        await dama_events.dispatch(createIndiceEvent, etlContextId);

        await createIndices(
          source_id,
          view_id,
          year,
          table_name?.toLowerCase(),
          sqlLog,
          dbConnection
        );

        logger.info("\nreached here ----- 6 -----");
        const correctGeoidsEvent = {
          type: `Tiger_dataset:${table_name}_CORRECT_GEOIDS_EVENT`,
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
          source_id,
          view_id,
          year,
          table_name?.toLowerCase(),
          sqlLog,
          dbConnection
        );

        logger.info("\nreached here ----- 7 -----");

        finalTableQueries.push(
          `SELECT wkb_geometry, geoid, ${
            table_name === "STATE" ? "stusps as name" : "name"
          }, ${year} as year, '${table_name?.toLowerCase()}' as tiger_type FROM temp.tl_${year}_${table_name}_s${source_id}_v${view_id}`
        );
        tempTableNames.push(
          `tl_${year}_${table_name}_s${source_id}_v${view_id}`
        );
        logger.info("\nreached here ----- 8 -----");

        await dbConnection.query("COMMIT;");

        // if (isNewSourceCreate) {
        //   logger.info("called inside setSourceMetadata Begin");
        //   await dbConnection.query({
        //     text: "CALL _data_manager_admin.initialize_dama_src_metadata_using_view( $1 )",
        //     values: [view_id],
        //   });
        //   isNewSourceCreate = false;
        //   logger.info("called inside setSourceMetadata End");
        // }

        // Create Mbtile
        const featureEditor = (feature) => {
          feature.tippecanoe = { "layer" : `${feature.properties.tiger_type}_${feature.properties.year}` };
          delete feature.properties.tiger_type;
          delete feature.properties.year;
          return feature;
        };
        await createViewMbtiles(view_id, source_id, etlContextId,
          {
            preserveColumns: ["geoid", "tiger_type", "year"],
            featureEditor,
          }
        );
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
        logger.info(`\nError Event ${JSON.stringify(errEvent, null, 3)} `);
        throw e;
      } finally {
        dbConnection.release();
      }
    }
  }

  dbConnection = await dama_db.getDbConnection();
  try {
    await dbConnection.query("BEGIN ;");
    await createViewTable(finalTableQueries, source_id, view_id, dbConnection);
    await dbConnection.query(
      `ALTER TABLE tiger.tl_s${source_id}_v${view_id} ADD COLUMN ogc_fid SERIAL PRIMARY KEY;`
    );
    await update_view({
      table_schema: "tiger",
      table_name: `tiger.tl_s${source_id}_v${view_id}`,
      view_id,
      dbConnection,
    });
    await dbConnection.query({
      text: "CALL _data_manager_admin.initialize_dama_src_metadata_using_view( $1 )",
      values: [view_id],
    });
    await dropTmpTables(tempTableNames, dbConnection);
    await dbConnection.query("COMMIT;");
  } catch (error) {
    await dbConnection.query("ROLLBACK;");
  } finally {
    dbConnection.release();
  }

  logger.info(`Reached here with: ${JSON.stringify(finalTableQueries)}`);
  const finalEvent = {
    type: "Tiger_dataset.FINAL",
    payload: {
      damaSourceId: source_id,
    },
    meta: {
      user_id,
      timestamp: new Date().toISOString(),
    },
  };

  logger.info(`\nFinal Event ${JSON.stringify(finalEvent, null, 3)} `);
  await dama_events.dispatch(finalEvent, etlContextId);
  return finalEvent;
}
