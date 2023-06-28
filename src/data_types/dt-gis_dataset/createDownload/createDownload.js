import { execSync } from "child_process";
import { existsSync, mkdirSync, createWriteStream, rmSync } from "fs";
import { join } from "path";

import {
  getPsqlCredentials,
  getConnectedNodePgClient,
} from "../../../data_manager/dama_db/postgres/PostgreSQL";

import dama_db from "../../../data_manager/dama_db";

import { dataDir } from "../../../constants";

const OutputTypes = {
  CSV: "CSV",
  ESRI_SHAPEFILE: "ESRI Shapefile",
  GEOJSON: "GeoJSON",
  GPKG: "GPKG",
}

export const outputTypeFileExtensions = {
  [OutputTypes.CSV]: "csv",
  [OutputTypes.ESRI_SHAPEFILE]: "",
  [OutputTypes.GEOJSON]: "geojson",
  [OutputTypes.GPKG]: "gpkg",
};

export default async function createDownloads(ctx) {
  
  let {
    // @ts-ignore
    params: {
      etlContextId,
      source_id,
      view_id
    },
    meta: {
      pgEnv,
      etl_context_id
    }
  } = ctx 
  
  console.log('Got to create download', etlContextId, source_id, view_id)

  let download = {}
  for (const outputType of Object.values(OutputTypes)) {
    download[outputType] = await createDownloadable(pgEnv, source_id, view_id, outputType);
  }

  await dama_db.query({
      text: `
        UPDATE data_manager.views
          SET metadata = COALESCE(metadata,'{}') || $1
          WHERE view_id = $2
        ;
      `,
      values: [{download}, view_id]
    },
    pgEnv
  );

  return {
    response: "Create Download Complete",
    pgEnv,
    etlContextId,
    source_id,
    view_id,
    download
  }

    
}

async function createDownloadable(pgEnv, source_id, view_id, outputType) {

  const { rows } = await dama_db.query(`
    SELECT
        a.name AS source_name,
        b.version AS view_version,
        b.data_table as data_table
      FROM data_manager.sources AS a
        INNER JOIN data_manager.views AS b
          ON (a.source_id = b.source_id)
      WHERE view_id = ${view_id};
  `, pgEnv);

  const {source_name, view_version, data_table } = rows[0]

  const fileNameBase = `${source_name}_${view_id}${view_version ? `_${view_version}` : ''}`
  const extension = outputTypeFileExtensions[outputType];
  const fileName = extension ? `${fileNameBase}.${extension}` : fileNameBase;
  const outputDir = `${dataDir}/${pgEnv}_${view_id}`
  const filePath = join(outputDir, fileName);

  //const outputDir = `${dataDir}/${view_id}`
  console.log('outputDir', outputDir)
  mkdirSync(outputDir, { recursive: true });
  
  console.log('filePath', filePath)
  if (outputType === OutputTypes.ESRI_SHAPEFILE) {
    mkdirSync(filePath, { recursive: true });
  }

  const { PGHOST, PGUSER, PGDATABASE, PGPASSWORD } = getPsqlCredentials(pgEnv);

  const creds = `host='${PGHOST}' user='${PGUSER}' dbname='${PGDATABASE}' password='${PGPASSWORD}'`;

    //console.log("=====", fileNameBase, "=====");

    try {
      const create = `
        set -e

        rm -rf "${fileName}"

        ogr2ogr \
          -f '${outputType}' \
          -t_srs 'EPSG:4326' \
          -skipfailures \
          -lco GEOMETRY_NAME=wkb_geometry \
          -nln "${fileNameBase}" \
          "${fileName}" \
          PG:"${creds}" \
          '${data_table}'

        echo

        ogrinfo -al -so "${fileName}"

        echo

        zip -rm "${fileName}.zip" "${fileName}"
      `;

      execSync(create, {
        cwd: outputDir,
        shell: "/bin/bash",
        //stdio: ["ignore", logStream, logStream],
      });

    } catch (err) {
      if (err) {
        console.error(err);
      }
      const fpath = join(outputDir, fileName);
      if (existsSync(fpath)) {
        rmSync(fpath, { recursive: true, force: true });
        rmSync(`${fpath}.zip`, { recursive: true, force: true });
      }
    }

    return `$HOST/files/${pgEnv}_${view_id}/${fileName}.zip`
    //logStream.end();
  }

  

  // NOTE: This is kept separate so we can QA the created downloadables before publishing them.
  // async updateDataManagerDownloadablesUrls() {
  //   const db = await this.getDbConnection();

  //   const viewsDataTablesMetadata = await this.getViewsDataTablesMetadata();

  //   for (const metadata of viewsDataTablesMetadata) {
  //     const { view_id } = metadata;

  //     const downloadMetadata = Object.values(OutputTypes).reduce(
  //       (acc, outputType) => {
  //         const { fileName } = this.getFileNameInfo(metadata, outputType);

  //         const extension = outputTypeFileExtensions[outputType];
  //         const url = `https://data.availabs.org/nysdot_freight_atlas/${fileName}.zip`;

  //         acc[extension] = url;

  //         return acc;
  //       },
  //       {}
  //     );

  //     await db.query(
  //       `
  //         UPDATE data_manager.views
  //           SET metadata = jsonb_set (metadata, ARRAY['download'], $1)
  //           WHERE id = $2
  //         ;
  //       `,
  //       [downloadMetadata, view_id]
  //     );
  //   }

  //   this.closeDbConnection();
  // }
