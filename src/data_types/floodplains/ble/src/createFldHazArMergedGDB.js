#!/usr/bin/env node

/*
ogr2ogr -F 'OpenFileGDB' merged_ble_fld_haz_ar.gdb merged_ble_fld_haz_ar.gpkg
ogrinfo merged_ble_fld_haz_ar.gdb/ -sql "ALTER TABLE S_Fld_Haz_Ar RENAME COLUMN SHAPE_Length TO SHAPE_Leng"
*/

require("ts-node").register();
require("tsconfig-paths").register();

const { execSync } = require("child_process");
const { existsSync, mkdirSync, rmSync } = require("fs");
const { basename, join } = require("path");

const _ = require("lodash");

const dama_db = require("../../../../data_manager/dama_db").default;

const {
  getPostgresConnectionString,
} = require("data_manager/dama_db/postgres/PostgreSQL");

const gdbs_meta = require("../gdbs_meta.json");

const { required_fields } = require("./merged_gdb_schema");

const pg_env = "dama_dev_1";

const extracted_dir = join(__dirname, "../initial_data/extracted");
const merged_datasets_dir = join(__dirname, "../merged");
const merge_work_dir = join(merged_datasets_dir, "tmp");

const merged_gpkg = join(merged_datasets_dir, "merged_ble_fld_haz_ar.gpkg");

const layer_name = "S_Fld_Haz_Ar";

mkdirSync(merged_datasets_dir, { recursive: true });

const gdbs = Object.keys(gdbs_meta);

function getGDBsWithFldHazArLayer() {
  const gdbs_with_fld_haz_ar_layer = [];

  for (const gdb of gdbs) {
    const { layers } = gdbs_meta[gdb];

    const fld_haz_ar_layer = layers.find(
      ({ name }) => name.toUpperCase() === "S_FLD_HAZ_AR"
    );

    if (fld_haz_ar_layer) {
      gdbs_with_fld_haz_ar_layer.push(gdb);
    }
  }

  return gdbs_with_fld_haz_ar_layer;
}

function getFldHazArLayerSchema(gdbs_with_fld_haz_ar_layer) {
  let fld_haz_ar_layer_schema = null;

  for (const gdb of gdbs_with_fld_haz_ar_layer) {
    const { layers } = gdbs_meta[gdb];

    const fld_haz_ar_layer = layers.find(
      ({ name }) => name.toUpperCase() === "S_FLD_HAZ_AR"
    );

    const { fields } = fld_haz_ar_layer;

    fields.sort((a, b) => a.name.localeCompare(b.name));

    fld_haz_ar_layer_schema = fld_haz_ar_layer_schema || fields;

    if (!_.isEqual(fields, fld_haz_ar_layer_schema)) {
      throw new Error(
        "INVARIANT BROKEN: Different schemas for S_FLD_HAZ_AR layers."
      );
    }
  }

  return fld_haz_ar_layer_schema;
}

function compareFldHazArLayerSchemaToRequired(fld_haz_ar_layer_schema) {
  const field_names = fld_haz_ar_layer_schema
    .map(({ name }) => name.toUpperCase())
    .sort();

  const missing = _.difference(required_fields, field_names);
  const extra = _.difference(field_names, required_fields);

  console.log(JSON.stringify({ missing, extra }, null, 4));

  if (
    !_.isEqual(
      {
        missing: ["GFID", "SHAPE_LENG"],
        extra: ["SHAPE_LENGTH"],
      },
      { missing, extra }
    )
  ) {
    throw new Error("INVARIANT BROKEN: Unexpected fld_haz_ar_layer schema.");
  }

  return { missing, extra };
}

function getTempGPKGInfoForGDB(gdb) {
  const gdb_path = join(extracted_dir, gdb);

  const name = basename(gdb)
    .replace(/\//g, "_")
    .replace(/\.gdb$/, "");

  const gpkg_name = `${name}.gpkg`;
  const gpkg_path = join(merge_work_dir, gpkg_name);

  return { name, gpkg_name, gpkg_path };
}

// We create the tmp GPKGs so we can extract, then merge, only the S_Fld_Haz_Ar layers.
// Apparently ogrmerge.py does not support specifying specific layers.
function createTmpGPKGs(gdbs_with_fld_haz_ar_layer) {
  // console.log(JSON.stringify({ gdbs_with_fld_haz_ar_layer }, null, 4));

  mkdirSync(merged_datasets_dir, { recursive: true });

  if (existsSync(merge_work_dir)) {
    rmSync(merge_work_dir, { recursive: true });
  }

  mkdirSync(merge_work_dir, { recursive: true });

  const gpkg_paths = [];

  for (const gdb of gdbs_with_fld_haz_ar_layer) {
    const gdb_path = join(extracted_dir, gdb);

    const { name, gpkg_path } = getTempGPKGInfoForGDB(gdb);

    if (existsSync(gpkg_path)) {
      throw new Error(`INVARIANT BROKEN: Dataset name collision ${name}.`);
    }

    const create_cmd = `ogr2ogr -preserve_fid -t_srs EPSG:6541 -F 'GPKG' ${gpkg_path} ${gdb_path} S_Fld_Haz_Ar`;

    execSync(create_cmd);

    gpkg_paths.push(gpkg_path);
  }

  return gpkg_paths;
}

/*
function createGFIDField(gdbs_with_fld_haz_ar_layer, schema_comparison) {
  for (const gdb of gdbs_with_fld_haz_ar_layer) {
    const { gpkg_path } = getTempGPKGInfoForGDB(gdb);

    const alter_stmt = `ALTER TABLE S_Fld_Haz_Ar ADD COLUMN GFID TEXT;`;
    const alter_cmd = `ogrinfo -dialect SQLite -sql '${alter_stmt}' ${gpkg_path}`;

    execSync(alter_cmd);

    console.log(JSON.stringify({ extra: schema_comparison.extra }, null, 4));

    // NOTE: Without using json_patch, SQLite throws "too many arguments on function json_object"
    const json_object_args = schema_comparison.extra
      .filter((fld) => !/^SHAPE_LENGTH$/.test(fld))
      .map((field) => `'${field}',${field}`);

    console.log(JSON.stringify({ json_object_args }, null, 4));

    const update_stmt = `
      UPDATE S_Fld_Haz_Ar
        SET GFID = json_object(
          'meta',         json_object(
                            'type',   'BLE::S_Fld_Haz_Ar',
                            'file',   '${gdb}',
                            'layer',  'S_Fld_Haz_Ar',
                            'rowid',  ROWID
                          ),
          'other_fields', json_object(${json_object_args})
        )
      ;
    `;

    const update_cmd = `ogrinfo -dialect SQLite -sql "${update_stmt}" ${gpkg_path}`;

    execSync(update_cmd);
  }
}
*/

function createGFIDField(gdbs_with_fld_haz_ar_layer) {
  for (const gdb of gdbs_with_fld_haz_ar_layer) {
    const { gpkg_path } = getTempGPKGInfoForGDB(gdb);

    const alter_stmt = `ALTER TABLE ${layer_name} ADD COLUMN GFID TEXT;`;
    const alter_cmd = `ogrinfo -dialect SQLite -sql '${alter_stmt}' ${gpkg_path}`;

    execSync(alter_cmd);

    const update_stmt = `UPDATE ${layer_name} SET GFID = ( 'BLE:${gdb}:${layer_name}:' || ROWID )`;

    const update_cmd = `ogrinfo -dialect SQLite -sql "${update_stmt}" ${gpkg_path}`;

    execSync(update_cmd);
  }
}

function createMergedGPKG(gpkg_paths) {
  // console.log(JSON.stringify({ gdbs_with_fld_haz_ar_layer }, null, 4));

  if (existsSync(merged_gpkg)) {
    rmSync(merged_gpkg);
  }

  const input = gpkg_paths.join(" ");

  const cmd = `ogrmerge.py -single -f 'GPKG' -o ${merged_gpkg} ${input} -nln '${layer_name}'`;

  execSync(cmd);

  return merged_gpkg;
}

function createMergedGDB() {
  execSync(
    "docker run --rm -v ${PWD}:/data avail/gis-analysis /data/src/create_merged_fld_haz_ar_gdb.sh"
  );
}

async function loadMergedGDB() {
  const creds = getPostgresConnectionString(pg_env);

  const gdb_path = join(__dirname, "../merged/merged_ble_fld_haz_ar.gdb");

  await dama_db.query(
    `
      CREATE SCHEMA IF NOT EXISTS floodplains ;
      DROP TABLE IF EXISTS floodplains.merged_ble_fld_haz_ar ;
    `,
    pg_env
  );

  const cmd = `
    ogr2ogr \\
      -F PostgreSQL \\
      PG:"${creds}" \\
      ${gdb_path} \\
      -nlt CONVERT_TO_LINEAR \\
      -nln 'floodplains.merged_ble_fld_haz_ar'
  `;

  execSync(cmd);
  console.log("loaded merged_ble_fld_haz_ar into Postgres db");
}

async function main() {
  const gdbs_with_fld_haz_ar_layer = getGDBsWithFldHazArLayer();

  const fld_haz_ar_layer_schema = getFldHazArLayerSchema(
    gdbs_with_fld_haz_ar_layer
  );

  const schema_comparison = compareFldHazArLayerSchemaToRequired(
    fld_haz_ar_layer_schema
  );

  const gpkg_paths = createTmpGPKGs(gdbs_with_fld_haz_ar_layer);
  createGFIDField(gdbs_with_fld_haz_ar_layer, schema_comparison);

  createMergedGPKG(gpkg_paths);
  createMergedGDB();

  await loadMergedGDB();
}

// getSourceWithLayerNamesSummary();
main();
