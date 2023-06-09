import { spawnSync, execSync } from "child_process";
import {
  existsSync,
  readFileSync,
  renameSync,
  unlinkSync,
  rmSync,
  readdirSync,
} from "fs";
import { join, format } from "path";

import dedent from "dedent";
import _ from "lodash";

import { NpmrdsDownloadName } from "data_types/npmrds/domain";

import {
  RitisExportNpmrdsDataSource,
  NpmrdsExportDownloadMeta,
  NpmrdsExportRequest,
  NpmrdsExportMetadata,
  NpmrdsExportTransformOutput,
} from "data_types/npmrds/dt-npmrds_travel_times_export_ritis/domain";

import { createDataDateRange } from "data_types/npmrds/utils/dates";

import { makeFileReadOnlySync } from "data_utils/files";

import { getNpmrdsExportMetadata } from "data_types/npmrds/utils/npmrds_export_metadata";

import getNpmrdsExportsDir from "../../../../utils/getNpmrdsExportsDir";

import getEtlWorkDir from "var/getEtlWorkDir";

// The mock main reuses the same NpmrdsTravelTimesExport dir.
// The readonly files throw errors when we try to overwrite them.
// This fuction will do a cleanup if the NODE_ENV === "development".
// FIXME: DOES NOT HANDLE CONCURRENT PROCESSING
function cleanupReadOnlyFiles(npmrds_download_name: NpmrdsDownloadName) {
  // if (process.env.NODE_ENV?.toLowerCase() !== "development") {
  // return;
  // }
  const etl_work_dir = getEtlWorkDir();

  console.log("REMOVING THE READONLY FILES");

  const re_1 = new RegExp(`^${npmrds_download_name}\\.(csv|sqlite3)(.zip)?$`);

  const re_2 = /^TMC_Identification\..*\.csv(\.zip)?$/;

  const toRemove = readdirSync(etl_work_dir).filter(
    (f) => re_1.test(f) || re_2.test(f)
  );

  for (const f of toRemove) {
    rmSync(join(etl_work_dir, f), { force: true });
  }
}

const getExportDataSourcePathObj = (
  npmrds_download_name: NpmrdsDownloadName,
  dataSource: RitisExportNpmrdsDataSource
) => ({
  dir: join(getNpmrdsExportsDir(), dataSource.toLowerCase()),
  base: `${npmrds_download_name}.zip`,
  name: npmrds_download_name,
  ext: ".zip",
});

const getExportDataSourcesPath = (
  npmrds_download_name: NpmrdsDownloadName,
  dataSource: RitisExportNpmrdsDataSource
) => format(getExportDataSourcePathObj(npmrds_download_name, dataSource));

export function getExportDataSourcesPathObjs(
  npmrds_download_name: NpmrdsDownloadName
) {
  const npmrdsAllVehiclesTravelTimesExport = getExportDataSourcePathObj(
    npmrds_download_name,
    RitisExportNpmrdsDataSource.ALL_VEHICLES
  );

  const npmrdsPassengerVehiclesTravelTimesExport = getExportDataSourcePathObj(
    npmrds_download_name,
    RitisExportNpmrdsDataSource.PASSENGER_VEHICLES
  );

  const npmrdsFreightTrucksTravelTimesExport = getExportDataSourcePathObj(
    npmrds_download_name,
    RitisExportNpmrdsDataSource.TRUCKS
  );

  return {
    npmrdsAllVehiclesTravelTimesExport,
    npmrdsPassengerVehiclesTravelTimesExport,
    npmrdsFreightTrucksTravelTimesExport,
  };
}

const getExportDataSourcesPaths = (npmrds_download_name: NpmrdsDownloadName) =>
  _.mapValues(getExportDataSourcesPathObjs(npmrds_download_name), (v) =>
    format(v)
  );

const getTmcIdentificationPathObj = (
  npmrds_download_name: NpmrdsDownloadName
) => {
  const [, prefix, year, suffix] = npmrds_download_name.match(
    /(^npmrdsx?_[a-z]{2})_from_(\d{4}).*(_v.*$)/
  )!;

  const meta = `${prefix}_${year}_${suffix}`;

  const name = `TMC_Identification.${meta}`;
  const ext = ".csv";
  const base = `${name}${ext}`;

  return { dir: getEtlWorkDir(), base, ext, name };
};

const getTmcIdentificationZipPathObj = (
  npmrds_download_name: NpmrdsDownloadName
) => {
  const { dir, base } = getTmcIdentificationPathObj(npmrds_download_name);

  return {
    dir,
    base: `${base}.zip`,
    ext: ".zip",
    name: base,
  };
};

const getNpmrdsTravelTimesCsvPathObj = (
  npmrds_download_name: NpmrdsDownloadName
) => {
  const ext = ".csv";
  const name = npmrds_download_name;
  const base = `${name}${ext}`;

  return { dir: getEtlWorkDir(), base, ext, name };
};

const getDbPathObj = (npmrds_download_name: NpmrdsDownloadName) => ({
  dir: getEtlWorkDir(),
  base: `${npmrds_download_name}.sqlite3`,
  name: npmrds_download_name,
  ext: ".sqlite3",
});

const getDbPath = (npmrds_download_name: NpmrdsDownloadName) =>
  format(getDbPathObj(npmrds_download_name));

function initializeDatabase(npmrds_download_name: NpmrdsDownloadName) {
  const sqlite_db_path = getDbPath(npmrds_download_name);

  const sql = readFileSync(join(__dirname, "./sql/create_tables.sql"), {
    encoding: "utf8",
  });

  if (existsSync(sqlite_db_path)) {
    unlinkSync(sqlite_db_path);
  }

  const { error, stderr } = spawnSync("sqlite3", [sqlite_db_path, sql], {
    encoding: "utf8",
  });

  if (error || stderr.length) {
    console.error(stderr);
    throw error || new Error(stderr);
  }

  return sqlite_db_path;
}

function extractTmcIdentificationZipFile(
  npmrds_download_name: NpmrdsDownloadName
) {
  const etl_work_dir = getEtlWorkDir();

  const exportDataSourcePathObj = getExportDataSourcePathObj(
    npmrds_download_name,
    RitisExportNpmrdsDataSource.ALL_VEHICLES
  );

  const exportDataSourcePath = format(exportDataSourcePathObj);

  const tmcIdentFileBase = "TMC_Identification.csv";
  const extractedTmcIdentPath = join(etl_work_dir, tmcIdentFileBase);

  execSync(`
    unzip -o \
        '${exportDataSourcePath}' \
        ${tmcIdentFileBase} \
        -d ${etl_work_dir}
  `);

  const tmcIdentificationPathObj =
    getTmcIdentificationPathObj(npmrds_download_name);

  const tmcIdentificationPath = format(tmcIdentificationPathObj);

  renameSync(extractedTmcIdentPath, tmcIdentificationPath);

  makeFileReadOnlySync(tmcIdentificationPath);

  const tmcIdentificationZipName = `${tmcIdentificationPathObj.base}.zip`;
  const tmcIdentificationZipPath = join(etl_work_dir, tmcIdentificationZipName);

  execSync(
    `
    zip \
        -9 \
        -m \
        ${tmcIdentificationZipName} \
        ${tmcIdentificationPathObj.base}
  `,
    { cwd: etl_work_dir, encoding: "utf8" }
  );

  makeFileReadOnlySync(tmcIdentificationZipPath);

  return tmcIdentificationZipPath;
}

function loadTmcIdentification(npmrds_export_metadata: NpmrdsExportMetadata) {
  const { name: npmrds_download_name, state } = npmrds_export_metadata;

  const sqlite_db_path = getDbPath(npmrds_download_name);

  console.log("loading TMC_Identification");
  console.time("loading TMC_Identification");

  const tmcIdentificationZipPathObj =
    getTmcIdentificationZipPathObj(npmrds_download_name);

  const tmcIdentificationZipPath = format(tmcIdentificationZipPathObj);

  execSync(`
    unzip \
        -p \
        '${tmcIdentificationZipPath}' \
      | sqlite3 -csv '${sqlite_db_path}' ".import '|cat -' tmc_identification"
  `);

  // Because the Canadian TMCs' state column is not the abbreviation.
  if (state === "qc") {
    const cmd = dedent(`
      sqlite3 \
        ${sqlite_db_path} \
        "
          UPDATE tmc_identification
            SET state = UPPER('${state}')
            WHERE ( UPPER(state) LIKE 'Q%' )
        "
    `);

    execSync(cmd);
  }

  if (state === "on") {
    const cmd = dedent(`
      sqlite3 \
        ${sqlite_db_path} \
        "
          UPDATE tmc_identification
            SET state = UPPER('${state}')
            WHERE ( UPPER(state) LIKE 'ON%' )
        "
    `);

    execSync(cmd);
  }

  console.timeEnd("loading TMC_Identification");
}

function loadNpmrdsTravelTimesData(npmrds_download_name: NpmrdsDownloadName) {
  const sqlite_db_path = getDbPath(npmrds_download_name);

  const awkScriptPath = join(
    __dirname,
    "./lib/order_columns_during_ingest.awk"
  );

  for (const dataSource of Object.values(RitisExportNpmrdsDataSource)) {
    console.log(`loading ${dataSource}`);

    console.time(`        ${dataSource}`);

    const zipPath = getExportDataSourcesPath(npmrds_download_name, dataSource);

    const cmd = dedent(`
      unzip \
          -p \
          '${zipPath}' \
          ${npmrds_download_name}.csv \
        | awk -f '${awkScriptPath}' \
        | tail -n +2 \
        | sqlite3 -csv '${sqlite_db_path}' ".import '|cat -' ${dataSource.toLowerCase()}"
    `);

    execSync(cmd);

    console.timeEnd(`        ${dataSource}`);
  }
}

function createCsv(npmrds_download_name: NpmrdsDownloadName) {
  const sqlite_db_path = getDbPath(npmrds_download_name);

  const npmrdsTravelTimesCsvPathObj =
    getNpmrdsTravelTimesCsvPathObj(npmrds_download_name);

  const npmrdsTravelTimesCsvPath = format(npmrdsTravelTimesCsvPathObj);

  const cmd = dedent(`
    sqlite3 \
      -header \
      -csv \
      ${sqlite_db_path} \
      '
        SELECT
            tmc,
            date,
            epoch,
            travel_time_all_vehicles,
            travel_time_passenger_vehicles,
            travel_time_freight_trucks,
            data_density_all_vehicles,
            data_density_passenger_vehicles,
            data_density_freight_trucks
          FROM npmrds_travel_times
          ORDER BY tmc, date, epoch
      ' \
      > ${npmrdsTravelTimesCsvPath}
  `);

  execSync(cmd);

  makeFileReadOnlySync(npmrdsTravelTimesCsvPath);

  const { dir, base } = npmrdsTravelTimesCsvPathObj;
  const zipFileName = `${base}.zip`;

  execSync(`zip -m ${zipFileName} ${base}`, { cwd: dir });

  const zipFilePath = join(dir, zipFileName);
  makeFileReadOnlySync(zipFilePath);

  return zipFilePath;
}

function loadMetadataTable(npmrds_export_metadata: NpmrdsExportMetadata) {
  const {
    name: npmrds_download_name,
    state,
    start_date,
    end_date,
    is_expanded,
    is_complete_month,
    is_complete_week,
  } = npmrds_export_metadata;

  const sqlite_db_path = getDbPath(npmrds_download_name);

  const [version_timestamp] = npmrds_download_name.match(/[0-9t]+$/i)!;

  const YYYY = version_timestamp.slice(0, 4);
  const MM = version_timestamp.slice(4, 6);
  const DD = version_timestamp.slice(6, 8);
  const hh = version_timestamp.slice(9, 11);
  const mm = version_timestamp.slice(11, 13);
  const ss = version_timestamp.slice(13, 15);

  const timestamp = `${YYYY}-${MM}-${DD}T${hh}:${mm}:${ss}`;

  const data_date_range = createDataDateRange(start_date, end_date);

  const {
    start: { year },
  } = data_date_range;

  const sql = `
    INSERT INTO metadata (
      name,
      state,
      year,
      start_date,
      end_date,
      is_expanded,
      is_complete_month,
      is_complete_week,
      download_timestamp
    )
      VALUES (
        '${npmrds_download_name}',
        '${state}',
        ${year},
        '${start_date}',
        '${end_date}',
        ${+is_expanded},
        ${+is_complete_month},
        ${+is_complete_week},
        '${timestamp}'
      ) ;
  `;

  const { error, stderr } = spawnSync("sqlite3", [sqlite_db_path, sql], {
    encoding: "utf8",
  });

  if (error || stderr.length) {
    console.error(stderr);
    throw error || new Error(stderr);
  }
}

function finalizeDatabase(npmrds_download_name: NpmrdsDownloadName) {
  const sqlite_db_path = getDbPath(npmrds_download_name);

  const loadSql = readFileSync(join(__dirname, "./sql/load_tables.sql"), {
    encoding: "utf8",
  });

  const { error: loadErr, stderr: loadStderr } = spawnSync(
    "sqlite3",
    [sqlite_db_path, loadSql],
    {
      encoding: "utf8",
    }
  );

  if (loadErr || loadStderr.length) {
    console.error(loadStderr);
    throw loadErr || new Error(loadStderr);
  }

  const cleanupSql = readFileSync(join(__dirname, "./sql/cleanup.sql"), {
    encoding: "utf8",
  });

  const { error: cleanupErr, stderr: cleanupStderr } = spawnSync(
    "sqlite3",
    [sqlite_db_path, cleanupSql],
    {
      encoding: "utf8",
    }
  );

  if (cleanupErr || cleanupStderr.length) {
    console.error(cleanupStderr);
    throw cleanupErr || new Error(cleanupStderr);
  }

  spawnSync("sqlite3", [sqlite_db_path, "VACUUM"]);
  spawnSync("sqlite3", [sqlite_db_path, "ANALYZE"]);

  // Make the SQLite file read-only
  makeFileReadOnlySync(sqlite_db_path);
}

function zipDatabase(npmrds_download_name: NpmrdsDownloadName) {
  const { dir, base } = getDbPathObj(npmrds_download_name);
  const zipName = `${base}.zip`;
  const zipPath = join(dir, zipName);

  // We don't remove the original SQLiteDB because we need it later to load the NpmrdsTravelTimesDb table.
  execSync(`zip ${zipName} ${base}`, { cwd: dir });

  makeFileReadOnlySync(zipPath);

  return zipPath;
}

export type TaskParams = NpmrdsExportDownloadMeta & {
  npmrds_export_request: NpmrdsExportRequest;
};

export default async function main(): Promise<NpmrdsExportTransformOutput> {
  const npmrds_export_metadata = getNpmrdsExportMetadata();

  const { name: npmrds_download_name } = npmrds_export_metadata;

  cleanupReadOnlyFiles(npmrds_download_name);

  const npmrdsTravelTimesSqliteDb = initializeDatabase(npmrds_download_name);

  // npmrdsTmcIdentificationCsv is actually the npmrdsTmcIdentificationCsvZip path
  const npmrdsTmcIdentificationCsv =
    extractTmcIdentificationZipFile(npmrds_download_name);

  const {
    npmrdsAllVehiclesTravelTimesExport,
    npmrdsPassengerVehiclesTravelTimesExport,
    npmrdsFreightTrucksTravelTimesExport,
  } = getExportDataSourcesPaths(npmrds_download_name);

  loadNpmrdsTravelTimesData(npmrds_download_name);

  loadTmcIdentification(npmrds_export_metadata);

  loadMetadataTable(npmrds_export_metadata);

  finalizeDatabase(npmrds_download_name);

  const npmrdsTravelTimesCsv = createCsv(npmrds_download_name);

  const npmrdsTravelTimesExportSqlite = zipDatabase(npmrds_download_name);

  // NOTE: Naming conventions based on DamaSourceNames for the respective files.

  // FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME
  //  After integrating this module into the DamaController, use ENUM for naming.
  return {
    npmrdsDownloadName: npmrds_download_name,

    npmrdsTravelTimesExportRitis: getEtlWorkDir(),

    npmrdsAllVehiclesTravelTimesExport,
    npmrdsPassengerVehiclesTravelTimesExport,
    npmrdsFreightTrucksTravelTimesExport,

    npmrdsTravelTimesSqliteDb,

    npmrdsTravelTimesExportSqlite,
    npmrdsTmcIdentificationCsv,
    npmrdsTravelTimesCsv,
  };
}
