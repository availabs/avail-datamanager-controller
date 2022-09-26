/*
    WARNING: There's lots of write-to-disk side-effects in order to allow
             back-and-forth between the web client and the backend integration process.
    TODO:
          1.  Save the original upload as-is (compressed) for archival purposes.
          2.  Add status: 'STAGED' | 'PUBLISHED' to the LayerTableDescriptor
                'PUBLISHED' cannot be ALTERED/DROPPED.
*/

import { spawn } from "child_process";

import {
  existsSync,
  readFileSync,
  writeFileSync,
  createReadStream,
  createWriteStream,
  mkdirSync,
} from "fs";

import { Readable } from "stream";

import {
  readFile as readFileAsync,
  writeFile as writeFileAsync,
  mkdir as mkdirAsync,
  rename as renameAsync,
  utimes as utimesAsync,
} from "fs/promises";

import { hostname } from "os";
import { join, basename, dirname, extname, relative } from "path";
import { createHash } from "crypto";
import { createGzip } from "zlib";
import { pipeline, PassThrough } from "stream";
import { promisify } from "util";

import unzipper from "unzipper";
import tar, { Headers as TarStreamEntryHeader } from "tar-stream";
import gunzip from "gunzip-maybe";
import pEvent from "p-event";
import stableStringify from "json-stable-stringify";
import pgFormat from "pg-format";
import QueryStream from "pg-query-stream";

import { v4 as uuidv4 } from "uuid";
import _ from "lodash";

import {
  DatasetTypes,
  getGeoDatasetMetadata,
  analyzeGeoDatasetLayer,
  TableDescriptor,
  generateCreateTableStatement,
  generateLoadTableStatement,
  loadTable,
  launderLayerName,
  launderFieldName,
  GeoDatasetMetadata,
} from "../../utils/GeospatialDataUtils";

import etlDir from "../../../../../src/constants/etlDir";

import {
  getPsqlCredentials,
  getConnectedNodePgClient,
} from "../../../../../src/data_manager/dama_db/postgres/PostgreSQL";

import tippecanoePath from "../../../../../src/data_utils/gis/tippecanoe/constants/tippecanoePath";
import installTippecanoe from "../../../../../src/data_utils/gis/tippecanoe/bin/installTippecanoe";

const pipelineAsync = promisify(pipeline);

export const CLEANED_HOSTNAME = hostname().replace(/^a-z0-9/gi, "");

export type ParsedShapefileIntegratorId = {
  cleanedHostname: string;
  workDirName: string;
};

export type DatasetFileStats = {
  path: string;
  size: number;
  mtime: Date;
  md5sum: string;
};

export type DatasetUploadMetadata = {
  id: string;
  datasetType: DatasetTypes;
  relativeDatasetPath: string;
  maxLastModifiedDateTimestamp: string;
  filesMetadata: DatasetFileStats[];
};

export enum DatasetLayerUploadStatus {
  "RECEIVED" = "RECEIVED",
  "STAGED" = "STAGED",
  "PUBLISHED" = "PUBLISHED",
}

export type DatasetArchivedEntry =
  | (unzipper.Entry & { lastModifiedDateTime: Date; size: number })
  | (PassThrough & {
      type: TarStreamEntryHeader["type"];
      lastModifiedDateTime: Date;
      size: number;
      path: TarStreamEntryHeader["name"];
    });

export default class GeospatialDatasetIntegrator {
  // protected _db?: NodePgClient | null;
  private workDirPath: string | null;
  private datasetPath: string | null;
  private _geoDatasetMetadata?: GeoDatasetMetadata;
  private _layerNameToId?: Record<string, number>;

  // id is used to resume processing after user feedback.
  // It is returned by this.receiveZippedShapefile when the user uploads a shapefile.
  constructor(private id: string | null = null) {
    // id, workDirPath, and datasetPath are set when the data is received.
    if (!id) {
      this.workDirPath = null;
      this.datasetPath = null;

      return;
    }

    // @ts-ignore
    const { cleanedHostname, workDirName } = this.parseId();

    if (cleanedHostname !== CLEANED_HOSTNAME) {
      throw new Error("Wrong hostname on id.");
    }

    this.workDirPath = join(etlDir, workDirName);

    const { relativeDatasetPath } = JSON.parse(
      readFileSync(this.datasetUploadMetadataPath, {
        encoding: "utf8",
      })
    );

    this.datasetPath = join(this.workDirPath, relativeDatasetPath);

    if (!existsSync(this.datasetPath)) {
      throw new Error("No previously uploaded dataset exist.");
    }
  }

  static createId(workDirPath: string) {
    return `${CLEANED_HOSTNAME}_${basename(workDirPath)}`;
  }

  protected verifyDatasetReceived() {
    if (!this.workDirPath) {
      throw new Error("Have not yet received geospatial dataset");
    }
  }

  protected get datasetUploadMetadataPath() {
    if (!this.workDirPath) {
      throw new Error("Have not yet received dataset");
    }

    return join(this.workDirPath, "dataset_upload_metadata.json");
  }

  // The id encodes the hostname and the workdir name.
  protected parseId(): ParsedShapefileIntegratorId | null {
    if (!this.id) {
      return null;
    }

    console.log("===> id:", this.id);
    const [cleanedHostname, workDirName] = this.id.split(/_/);

    return {
      cleanedHostname,
      workDirName,
    };
  }

  async receiveDataset(fname: string, readStream: Readable) {
    let workDirPath: string;
    while (true) {
      const workDirName = uuidv4();
      workDirPath = join(etlDir, workDirName);

      if (existsSync(workDirPath)) {
        await new Promise((resolve) => process.nextTick(resolve));
        continue;
      }

      mkdirSync(workDirPath, { recursive: true });
      break;
    }

    const fpath = join(workDirPath, fname);
    const ws = createWriteStream(fpath);

    await pipelineAsync(readStream, ws);

    this.workDirPath = workDirPath;

    let iter: AsyncGenerator<DatasetArchivedEntry>;

    const zipRE = /\.zip$/i;
    const tarRE = /\.tar$|\.tar.gz$|\.tgz$/i;
    if (zipRE.test(fname)) {
      iter = this.makeZipDatasetEntriesAsyncGenerator(fpath);
    } else if (tarRE.test(fname)) {
      iter = this.makeTarDatasetEntriesAsyncGenerator(fpath);
    } else {
      throw new Error("receiveDataset ONLY supports ZIP and TAR archives.");
    }

    await this.injestDatasetEntries(iter);
    return this.id;
  }

  protected async injestDatasetEntries(
    datasetEntriesIter: AsyncGenerator<DatasetArchivedEntry>
  ) {
    const workDirPath = <string>this.workDirPath;

    const uploadTimestamp = new Date().toISOString();

    const datasetDir = join(workDirPath, "dataset");

    await mkdirAsync(datasetDir);

    // id is assigned to the integration
    const id = GeospatialDatasetIntegrator.createId(workDirPath);

    let maxLastModifiedDateTime: Date | null = null;

    let datasetType: DatasetTypes | null = null;
    let datasetSubPath = "";
    let datasetFileName = "";

    const filesMetadata: DatasetFileStats[] = [];

    const fingerprintHash = createHash("md5");
    fingerprintHash.update(stableStringify(filesMetadata));
    const fingerprint = fingerprintHash.digest("hex");

    for await (const entry of datasetEntriesIter) {
      const { path, lastModifiedDateTime, size, type } = entry;

      // @ts-ignore
      if (lastModifiedDateTime > maxLastModifiedDateTime) {
        maxLastModifiedDateTime = <Date>lastModifiedDateTime;
      }

      if (type === "Directory") {
        await mkdirAsync(join(<string>datasetDir, path));
      } else {
        // If we havent't yet determined the datasetType, try using file extension.
        if (!datasetType) {
          switch (extname(path).toLowerCase()) {
            case ".shp":
              datasetType = DatasetTypes.ESRI_Shapefile;
              break;
            case ".gdbtable":
              datasetType = DatasetTypes.FileGDB;
              break;
            case ".gpkg":
              datasetType = DatasetTypes.GPKG;
              datasetFileName = basename(path);
              break;
            case ".geojson":
              datasetType = DatasetTypes.GeoJSON;
              datasetFileName = basename(path);
              break;
            default:
          }

          // datasetType was set in above switch statement.
          if (datasetType) {
            // We may be in a subdirectory within the ZIP archive.
            datasetSubPath = dirname(path);
          }
        }

        const hash = createHash("md5");

        const I = new PassThrough();

        I.on("data", (d) => hash.update(d));

        const fpath = join(datasetDir, path);
        const ws = createWriteStream(fpath);

        const done = new Promise((resolve) => {
          ws.once("close", resolve);
        });
        // await pipelineAsync(entry, I, createWriteStream(fpath));

        entry.pipe(I).pipe(ws);

        await done;

        await utimesAsync(fpath, lastModifiedDateTime, lastModifiedDateTime);

        hash.end();
        const md5sum = hash.digest("hex");

        filesMetadata.push({
          path,
          mtime: lastModifiedDateTime,
          size,
          md5sum,
        });
      }
    }

    let datasetPath: string | null = null;
    switch (datasetType) {
      case DatasetTypes.ESRI_Shapefile:
        datasetPath = join(datasetDir, datasetSubPath);
        break;
      case DatasetTypes.FileGDB:
        datasetPath = join(datasetDir, datasetSubPath);
        if (!/gdb$/i.test(datasetPath)) {
          await renameAsync(datasetPath, `${datasetPath}.gdb`);
          datasetPath = `${datasetPath}.gdb`;
        }
        break;
      case DatasetTypes.GPKG:
      case DatasetTypes.GeoJSON:
        datasetPath = join(datasetDir, datasetSubPath, datasetFileName);
        break;
      default:
    }

    const datasetMetadata = {
      id,
      fingerprint,
      datasetType,
      relativeDatasetPath: datasetPath && relative(workDirPath, datasetPath),
      uploadTimestamp,
      maxLastModifiedDateTimeStamp:
        maxLastModifiedDateTime && maxLastModifiedDateTime.toISOString(),
      filesMetadata,
    };

    // Now make these visible outside the method.
    this.id = id;
    this.datasetPath = datasetPath;

    await writeFileAsync(
      this.datasetUploadMetadataPath,
      JSON.stringify(datasetMetadata),
      { encoding: "utf8" }
    );

    // We do this here so that the geoDatasetMetadata is available in later sync methods
    await this.getGeoDatasetMetadata();

    return datasetMetadata;
  }

  protected async *makeZipDatasetEntriesAsyncGenerator(
    fpath: string
  ): AsyncGenerator<DatasetArchivedEntry> {
    // FIXME: Should probably allow re-uploading under the same Id
    //        otherwise we would just collect failed uploads.
    if (this.id) {
      throw new Error("Already received geospatial dataset");
    }

    const rs = createReadStream(fpath);
    const parse = unzipper.Parse({ forceStream: true });

    const zip = rs.pipe(parse);

    for await (const entry of zip) {
      // @ts-ignore
      const {
        vars: { lastModifiedDateTime, uncompressedSize: size },
      } = entry;
      entry.lastModifiedDateTime = lastModifiedDateTime;
      entry.size = size;

      yield <DatasetArchivedEntry>entry;
    }
  }

  protected async *makeTarDatasetEntriesAsyncGenerator(
    fpath: string
  ): AsyncGenerator<DatasetArchivedEntry> {
    const rs = createReadStream(fpath);
    const extract = tar.extract();

    const entryAsyncIter = pEvent.iterator(extract, "entry", {
      multiArgs: true,
      resolutionEvents: ["finish"],
    });

    rs.pipe(gunzip()).pipe(extract);

    let header: TarStreamEntryHeader;
    let stream: Readable;
    let next: Function;
    // @ts-ignore
    for await ([header, stream, next] of entryAsyncIter) {
      const type = header.type === "file" ? "File" : "Directory";
      const path = header.name;
      const lastModifiedDateTime = header.mtime;
      const size = header.size;

      Object.assign(stream, { type, path, lastModifiedDateTime, size });

      const done =
        header.type === "file"
          ? new Promise<void>((resolve) => stream.once("end", resolve))
          : Promise.resolve();

      // @ts-ignore
      yield <DatasetArchivedEntry>stream;
      await done;
      next();
    }
  }

  protected get geoDatasetMetadataPath() {
    if (!this.workDirPath) {
      throw new Error("Have not yet received dataset");
    }

    return join(this.workDirPath, "geodataset_metadata.json");
  }

  // Layers get subWorkDirs. Because users may modify normalizedName,
  // we use immutable number based Ids for the directory naming scheme.
  protected getDatasetLayerWorkDir(layerName: string) {
    if (!this.workDirPath) {
      throw new Error("Have not yet received dataset");
    }

    const layerId = this.layerNameToId[layerName];
    const layerDirName = `layer_${layerId}`;

    const layerDir = join(this.workDirPath, layerDirName);

    mkdirSync(layerDir, { recursive: true });

    return layerDir;
  }

  protected getDatasetLayerLogsDir(layerName: string) {
    const dir = join(this.getDatasetLayerWorkDir(layerName), "logs");

    mkdirSync(dir, { recursive: true });

    return dir;
  }

  protected getDatasetLayerUploadStatusFilePath(layerName: string) {
    return join(this.getDatasetLayerWorkDir(layerName), "STATUS");
  }

  protected setDatasetLayerUploadStatus(
    layerName: string,
    status: DatasetLayerUploadStatus
  ) {
    writeFileSync(this.getDatasetLayerUploadStatusFilePath(layerName), status);
  }

  protected getDatasetLayerUploadStatus(
    layerName: string
  ): DatasetLayerUploadStatus {
    const fpath = this.getDatasetLayerUploadStatusFilePath(layerName);

    if (!existsSync(fpath)) {
      const status = DatasetLayerUploadStatus.RECEIVED;
      this.setDatasetLayerUploadStatus(layerName, status);
      return status;
    }

    return <DatasetLayerUploadStatus>readFileSync(fpath, {
      encoding: "utf8",
    });
  }

  protected getGeoDatasetLayerAnalysisPath(layerName: string) {
    const layerDir = this.getDatasetLayerWorkDir(layerName);

    const fpath = join(layerDir, "layer_analysis.json");

    return fpath;
  }

  async getGeoDatasetMetadata() {
    const fpath = this.geoDatasetMetadataPath;

    if (this._geoDatasetMetadata) {
      return <GeoDatasetMetadata>this._geoDatasetMetadata;
    }

    if (existsSync(fpath)) {
      const d = await readFileAsync(fpath, {
        encoding: "utf8",
      });

      this._geoDatasetMetadata = JSON.parse(d);

      return <GeoDatasetMetadata>this._geoDatasetMetadata;
    }

    this._geoDatasetMetadata = await getGeoDatasetMetadata(
      <string>this.datasetPath
    );

    await writeFileAsync(fpath, JSON.stringify(this._geoDatasetMetadata));

    this.layerNameToId = this._geoDatasetMetadata.layers.reduce(
      (acc: Record<string, number>, { layerName, layerId }) => {
        acc[layerName] = layerId;
        return acc;
      },
      {}
    );

    return <GeoDatasetMetadata>this._geoDatasetMetadata;
  }

  get layerNameToIdFilePath() {
    if (!this.workDirPath) {
      throw new Error("Have not yet received dataset");
    }

    return join(this.workDirPath, "layerNameToId.json");
  }

  // @ts-ignore
  protected set layerNameToId(layerNameToId: Record<string, number>) {
    this._layerNameToId = layerNameToId;

    writeFileSync(
      this.layerNameToIdFilePath,
      JSON.stringify(this._layerNameToId, null, 4),
      { encoding: "utf8" }
    );
  }

  // @ts-ignore
  get layerNameToId() {
    if (this._layerNameToId) {
      return this._layerNameToId;
    }

    if (!existsSync(this.layerNameToIdFilePath)) {
      throw new Error("Have not yet received the Geospatial Dataset");
    }

    this._layerNameToId = <Record<string, number>>(
      JSON.parse(readFileSync(this.layerNameToIdFilePath, { encoding: "utf8" }))
    );

    return this._layerNameToId;
  }

  async getGeoDatasetLayerAnalysis(layerName: string) {
    this.verifyDatasetReceived();

    const fpath = <string>this.getGeoDatasetLayerAnalysisPath(layerName);

    if (existsSync(fpath)) {
      const d = await readFileAsync(fpath, {
        encoding: "utf8",
      });

      return JSON.parse(d);
    }

    const analysis = await analyzeGeoDatasetLayer(
      <string>this.datasetPath,
      layerName
    );

    await writeFileAsync(fpath, JSON.stringify(analysis));

    // get will write RECEIVED if STATUS file does not exist
    this.getDatasetLayerUploadStatus(layerName);

    return analysis;
  }

  protected async persistDatasetLayerCreateTableSql(
    layerName: string,
    sql: string | Buffer,
    datetime: Date = new Date()
  ) {
    const dir = this.getDatasetLayerLogsDir(layerName);

    const tstamp = datetime.toISOString().replace(/[^0-9a-z]/gi, "");

    const fpath = join(dir, `create_table.${tstamp}.sql`);

    await writeFileAsync(fpath, sql);
  }

  protected generateGeoDatasetLayerCreateTableStatement(
    tableDescriptor: TableDescriptor
  ) {
    return generateCreateTableStatement(tableDescriptor);
  }

  protected getLayerTableDescriptorPath(layerName: string) {
    const layerDir = this.getDatasetLayerWorkDir(layerName);
    const fpath = join(layerDir, "table_descriptor.json");

    return fpath;
  }

  async getLayerTableDescriptor(layerName: string) {
    const fpath = this.getLayerTableDescriptorPath(layerName);

    if (!existsSync(fpath)) {
      return this.generateGeoDatasetLayerDefaultDatabaseTableDescriptor(
        layerName
      );
    }

    const d = await readFileAsync(fpath, { encoding: "utf8" });

    const tableDescriptor = JSON.parse(d);

    return tableDescriptor;
  }

  protected getLayerLoadTableSqlPath(layerName: string) {
    const layerDir = this.getDatasetLayerWorkDir(layerName);
    const fpath = join(layerDir, "load_table.sql");

    return fpath;
  }

  async generateGeoDatasetLayerDefaultDatabaseTableDescriptor(
    layerName: string,
    overwrite = false
  ): Promise<TableDescriptor> {
    this.verifyDatasetReceived();

    const metadata = await this.getGeoDatasetMetadata();

    const layerMetadata = metadata.layers.find(
      ({ layerName: name }) => name === layerName
    );

    if (!layerMetadata) {
      throw new Error(`Layer ${layerName} not found in Geo Dataset`);
    }

    const analysis = await this.getGeoDatasetLayerAnalysis(layerName);

    const tableSchema = "ui_loaded_data_sources";
    const tableName = launderLayerName(layerName);

    const {
      layerFieldsAnalysis: { schemaAnalysis },
      layerGeometriesAnalysis: {
        requiresPromoteToMulti,
        requiresForcedPostGisDimension,
        commonPostGisGeometryType,
      },
    } = analysis;

    const columnTypes = schemaAnalysis.map(({ key, summary: { db_type } }) => ({
      key,
      col: launderFieldName(key),
      db_type: db_type || "TEXT", // if no values seen, db_type is null
    }));

    layerMetadata.fieldsMetadata.forEach(({ name, type }) => {
      if (type === "binary") {
        const colType = columnTypes.find(({ key }) => key === name);
        console.log("==>", name, type, colType);
        colType.db_type = "BYTEA";
      }
    });

    const tableDescriptor = {
      layerName,
      tableSchema,
      tableName,
      columnTypes,
      requiresPromoteToMulti,
      requiresForcedPostGisDimension,
      promoteToMulti: requiresPromoteToMulti,
      forcePostGisDimension: requiresForcedPostGisDimension,
      postGisGeometryType: commonPostGisGeometryType,
    };

    // If a TableDescriptor file does not exist for this layer,
    // write this default TableDescriptor to disk.
    await this.persistLayerTableDescriptor(tableDescriptor, overwrite);

    return tableDescriptor;
  }

  async persistLayerTableDescriptor(
    tableDescriptor: TableDescriptor,
    overwrite = true
  ) {
    const { layerName } = tableDescriptor;

    const layerTableDescriptorPath =
      this.getLayerTableDescriptorPath(layerName);

    if (overwrite || !existsSync(layerTableDescriptorPath)) {
      await writeFileAsync(
        layerTableDescriptorPath,
        JSON.stringify(tableDescriptor, null, 4)
      );
    }
  }

  protected async persistDatasetLayerCreateTableError(
    layerName: string,
    err: Error,
    datetime: Date = new Date()
  ) {
    const dir = this.getDatasetLayerLogsDir(layerName);
    const timestamp = datetime.toISOString();
    const tstamp = timestamp.replace(/[^0-9a-z]/gi, "");

    const fpath = join(dir, `create_table_error.${tstamp}.json`);

    await writeFileAsync(fpath, JSON.stringify(err));
  }

  // NOTE: Normal usage is loadTable. This method exists for debugging/troubleshooting/testing.
  protected async createGeoDatasetLayerTable({
    layerName,
    pgEnv,
  }: {
    layerName: string;
    pgEnv: string;
  }) {
    const tableDescriptor = await this.getLayerTableDescriptor(layerName);

    const sql =
      this.generateGeoDatasetLayerCreateTableStatement(tableDescriptor);

    const datetime = new Date();
    this.persistDatasetLayerCreateTableSql(layerName, sql, datetime);

    const db = await getConnectedNodePgClient(pgEnv);

    try {
      await db.query(sql);
    } catch (err) {
      await this.persistDatasetLayerCreateTableError(
        layerName,
        <Error>err,
        datetime
      );
    } finally {
      db.end();
    }
  }

  protected async generateGeoDatasetLayerLoadTableStatement(layerName: string) {
    // When loading table, we MUST use the TableDescriptor written to disk
    // after table creation.
    const tableDescriptor = await this.getLayerTableDescriptor(layerName);

    return generateLoadTableStatement(tableDescriptor);
  }

  protected async persistLoadTableMetadata(
    loadTableMetadata: any,
    datetime: Date = new Date()
  ) {
    const {
      tableDescriptor: { layerName },
    } = loadTableMetadata;

    const dir = this.getDatasetLayerLogsDir(layerName);
    const timestamp = datetime.toISOString();
    const tstamp = timestamp.replace(/[^0-9a-z]/gi, "");

    const fpath = join(dir, `load_table_metadata.${tstamp}.json`);

    await writeFileAsync(
      fpath,
      JSON.stringify({ timestamp, ...loadTableMetadata })
    );
  }

  protected async persistDatasetLayerLoadError(
    layerName: string,
    err: Error,
    datetime: Date = new Date()
  ) {
    const dir = this.getDatasetLayerWorkDir(layerName);
    const timestamp = datetime.toISOString();
    const tstamp = timestamp.replace(/[^0-9a-z]/gi, "");

    const fpath = join(dir, `load_table_error.${tstamp}.json`);

    await writeFileAsync(fpath, JSON.stringify(err));
  }

  // NOTE: Will DROP existing table if not PUBLISHED.
  async loadTable({ layerName, pgEnv }: { layerName: string; pgEnv: string }) {
    // TODO:  ui_loaded_data_sources-local data_manager tables
    //        updated for the new layer.

    if (
      this.getDatasetLayerUploadStatus(layerName) ===
      DatasetLayerUploadStatus.PUBLISHED
    ) {
      throw new Error("PUBLISHED dataset tables are immutable.");
    }

    const tableDescriptor = await this.getLayerTableDescriptor(layerName);

    const datetime = new Date();

    try {
      const loadTableMetadata = await loadTable(
        <string>this.datasetPath,
        tableDescriptor,
        pgEnv
      );

      this.setDatasetLayerUploadStatus(
        layerName,
        DatasetLayerUploadStatus.STAGED
      );

      const { PGHOST, PGDATABASE } = getPsqlCredentials(pgEnv);

      await this.persistLoadTableMetadata(
        { PGHOST, PGDATABASE, ...loadTableMetadata },
        datetime
      );

      await this.persistDatasetLayerCreateTableSql(
        layerName,
        loadTableMetadata.createTableSql,
        datetime
      );
    } catch (err) {
      await this.persistDatasetLayerLoadError(layerName, <Error>err, datetime);
      throw err;
    }
  }

  getLayerGeometriesGeoJSONPath(layerName: string) {
    const dir = this.getDatasetLayerWorkDir(layerName);
    return join(dir, "layer_geometries.geojson.gz");
  }

  async dumpGeoDatasetLayerGeometriesGeoJSON({
    layerName,
    pgEnv,
  }: {
    layerName: string;
    pgEnv: string;
  }) {
    const { tableSchema, tableName } = await this.getLayerTableDescriptor(
      layerName
    );

    const db = await getConnectedNodePgClient(pgEnv);

    try {
      // https://gis.stackexchange.com/a/191446
      await new Promise(async (resolve, reject) => {
        const sql = pgFormat(
          `
            SELECT jsonb_build_object(
              'type',       'Feature',
              'id',         ogc_fid,
              'geometry',   ST_AsGeoJSON(wkb_geometry)::JSON,
              'properties', jsonb_build_object('id', ogc_fid)
            ) AS feature FROM (SELECT ogc_fid, wkb_geometry FROM %I.%I) row;
          `,
          tableSchema,
          tableName
        );

        const q = new QueryStream(sql);
        const qstream = db.query(q);
        const gzip = createGzip();

        const fpath = this.getLayerGeometriesGeoJSONPath(layerName);
        const ws = createWriteStream(fpath);

        gzip.once("error", reject);
        ws.once("error", reject);
        ws.once("close", resolve);

        gzip.pipe(ws);

        for await (const { feature } of qstream) {
          const good = gzip.write(`${JSON.stringify(feature)}\n`);

          if (!good) {
            // console.log("awaiting drain");
            await new Promise((rslv) => gzip.once("drain", rslv));
            // console.log("drained");
          }
        }

        gzip.end();
      });
    } catch (err) {
      // console.error(err)
      throw err;
    } finally {
      await db.end();
    }
  }

  async *makeGeoJSONAsyncIter({
    layerName,
    pgEnv,
  }: {
    layerName: string;
    pgEnv: string;
  }) {
    const { tableSchema, tableName } = await this.getLayerTableDescriptor(
      layerName
    );

    const db = await getConnectedNodePgClient(pgEnv);

    try {
      // https://gis.stackexchange.com/a/191446
      const sql = pgFormat(
        `
            SELECT jsonb_build_object(
              'type',       'Feature',
              'id',         ogc_fid,
              'properties', to_jsonb(row) - 'wkb_geometry',
              'geometry',   ST_AsGeoJSON(wkb_geometry)::JSONB
            ) AS feature FROM (SELECT * FROM %I.%I) row;
          `,
        tableSchema,
        tableName
      );

      const q = new QueryStream(sql);
      const qstream = db.query(q);

      for await (const { feature } of qstream) {
        yield feature;
      }
    } catch (err) {
      // console.error(err)
      throw err;
    } finally {
      await db.end();
    }
  }

  async getFeatureProperties({
    layerName,
    featureId,
    pgEnv,
  }: {
    layerName: string;
    featureId: number;
    pgEnv: string;
  }) {
    const { tableSchema, tableName } = await this.getLayerTableDescriptor(
      layerName
    );

    const db = await getConnectedNodePgClient(pgEnv);

    try {
      // https://gis.stackexchange.com/a/191446
      const sql = pgFormat(
        `
            SELECT
                (to_jsonb(row) - 'wkb_geometry') AS properties
              FROM (SELECT * FROM %I.%I) row
              WHERE ogc_fid = $1;
          `,
        tableSchema,
        tableName,
        featureId
      );

      const {
        rows: [{ properties = null } = {}],
      } = await db.query(sql, [featureId]);

      return properties;
    } catch (err) {
      // console.error(err)
      throw err;
    } finally {
      await db.end();
    }
  }

  getLayerMBTilesPath(layerName: string) {
    const dir = this.getDatasetLayerWorkDir(layerName);
    return join(dir, "layer.mbtiles");
  }

  protected getLayerTippecanoeLogPath(
    layerName: string,
    datetime: Date = new Date()
  ) {
    const timestamp = datetime.toISOString();
    const tstamp = timestamp.replace(/[^0-9a-z]/gi, "");

    const dir = this.getDatasetLayerLogsDir(layerName);
    const fpath = join(dir, `tippecanoe.${tstamp}.json`);

    return fpath;
  }

  protected async persistTippecanoeLog(
    layerName: string,
    output: { stdout: string; stderr: string }
  ) {
    const fpath = this.getLayerTippecanoeLogPath(layerName);
    await writeFileAsync(fpath, JSON.stringify(output));
  }

  async createGeoDatasetLayerMBTiles(layerName: string) {
    const { tableName } = await this.getLayerTableDescriptor(layerName);

    const geojsonFilePath = this.getLayerGeometriesGeoJSONPath(layerName);
    if (!existsSync(geojsonFilePath)) {
      console.log({ geojsonFilePath });
      throw new Error(
        "You must first create the GeoJSON file using dumpGeoDatasetLayerGeometriesGeoJSON"
      );
    }

    const mbtilesFilePath = this.getLayerMBTilesPath(layerName);

    let success: Function;
    let fail: Function;

    console.log("tippecanoePath:", tippecanoePath);
    if (!existsSync(tippecanoePath)) {
      console.log("Install tippecanoe");
      await installTippecanoe();
    }

    const done = new Promise((resolve, reject) => {
      success = resolve;
      fail = reject;
    });

    const tippecanoeArgs = [
      "--no-progress-indicator",
      "--no-feature-limit",
      "--no-tile-size-limit",
      "-r1",
      "--force",
      "--layer",
      tableName,
      "-o",
      mbtilesFilePath,
      geojsonFilePath,
    ];

    const tippecanoeCProc = spawn(tippecanoePath, tippecanoeArgs, {
      stdio: "pipe",
    })
      // @ts-ignore
      .once("error", fail)
      // @ts-ignore
      .once("close", success);

    let stdout = "";

    /* eslint-disable-next-line no-unused-expressions */
    tippecanoeCProc.stdout?.on("data", (data) => {
      process.stdout.write(data);
      stdout += data.toString();
    });

    let stderr = "";

    /* eslint-disable-next-line no-unused-expressions */
    tippecanoeCProc.stderr?.on("data", (data) => {
      process.stderr.write(data);
      stderr += data.toString();
    });

    await done;

    await this.persistTippecanoeLog(layerName, { stdout, stderr });
  }

  async publishLayerTable(layerName: string) {
    // TODO: UPSERT data_manager tables. Need to verify same DB as STAGED.

    if (
      this.getDatasetLayerUploadStatus(layerName) ===
      DatasetLayerUploadStatus.RECEIVED
    ) {
      throw new Error(
        `${layerName} has not yet been loaded into the database.`
      );
    }

    this.setDatasetLayerUploadStatus(
      layerName,
      DatasetLayerUploadStatus.PUBLISHED
    );
  }
}
