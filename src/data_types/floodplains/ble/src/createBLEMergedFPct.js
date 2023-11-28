#!/usr/bin/env node

/*
ogr2ogr -F 'OpenFileGDB' s_fp_pct.gdb s_fp_pct.gpkg
ogrinfo s_fp_pct.gdb/ -sql "ALTER TABLE S_Fld_Haz_Ar RENAME COLUMN SHAPE_Length TO SHAPE_Leng"
*/

require("ts-node").register();
require("tsconfig-paths").register();

const { execSync } = require("child_process");
const { existsSync, mkdirSync, rmSync } = require("fs");
const { basename, join } = require("path");

const _ = require("lodash");

const gdbs_meta = require("../gdbs_meta.json");

const dama_db = require("../../../../data_manager/dama_db").default;

const {
  getPostgresConnectionString,
} = require("data_manager/dama_db/postgres/PostgreSQL");

const { gdb_fields, required_fields } = require("./merged_gdb_schema");

const pg_env = "dama_dev_1";

const extracted_dir = join(__dirname, "../initial_data/extracted");
const merged_fp_pct_dir = join(__dirname, "../merged/fp_pct");
const merge_work_dir = join(merged_fp_pct_dir, "tmp_fp_pct");

const merged_gpkg = join(merged_fp_pct_dir, "merged_ble_fp_pcts.gpkg");

const merged_gdb_path = join(
  __dirname,
  "../merged/fp_pct/merged_ble_fp_pcts.gdb"
);

mkdirSync(merged_fp_pct_dir, { recursive: true });

const gdbs = Object.keys(gdbs_meta);

const fp_pct_layer_names = ["FP_01PCT", "FP_0_2PCT"];

function getBLEGeoDatabases() {
  const seen_fp_layer_names = new Set();

  const gdbs_with_fp_pct_layer = [];

  for (const gdb of gdbs) {
    const { layers } = gdbs_meta[gdb];

    const fp_pct_layers = layers.filter(({ name }) =>
      /FP[0-2_]+PCT$/i.test(name)
    );

    if (fp_pct_layers.length === 0) {
      continue;
    }

    if (fp_pct_layers.length !== 2) {
      throw new Error(
        `INVARIANT BROKEN: There should be a 1% and 2% layer. Got: ${fp_pct_layers}.`
      );
    }

    for (const layer of fp_pct_layers) {
      seen_fp_layer_names.add(layer.name);
    }

    gdbs_with_fp_pct_layer.push(gdb);
  }

  const uniq_fp_pct_layer_names = [...seen_fp_layer_names].sort();

  if (!_.isEqual(uniq_fp_pct_layer_names, fp_pct_layer_names)) {
    throw new Error(
      `INVARIANT BROKEN: The expeced FP_PCT layer names are ${fp_pct_layer_names}. Got ${uniq_fp_pct_layer_names}.`
    );
  }

  return gdbs_with_fp_pct_layer;
}

function getFpPctLayerSchema(gdbs_with_fp_pct_layer) {
  let fp_pct_layer_schema = null;

  for (const gdb of gdbs_with_fp_pct_layer) {
    const { layers } = gdbs_meta[gdb];

    const fp_pct_layers = layers.filter(({ name }) =>
      /FP[0-2_]+PCT$/i.test(name)
    );

    for (const fp_pct_layer of fp_pct_layers) {
      const { fields } = fp_pct_layer;

      fields.sort((a, b) => a.name.localeCompare(b.name));

      fp_pct_layer_schema = fp_pct_layer_schema || fields;

      if (!_.isEqual(fields, fp_pct_layer_schema)) {
        throw new Error(
          "INVARIANT BROKEN: Different schemas for S_FLD_HAZ_AR layers."
        );
      }
    }
  }

  return fp_pct_layer_schema;
}

function compareFpPctLayerSchemaToRequired(fp_pct_layer_schema) {
  const field_names = fp_pct_layer_schema
    .map(({ name }) => name.toUpperCase())
    .sort();

  const missing = _.difference(required_fields, field_names);
  const extra = _.difference(field_names, required_fields);

  console.log(JSON.stringify({ missing, extra }, null, 4));

  if (
    !_.isEqual(
      {
        missing: [
          // "OGC_FID",
          "DFIRM_ID",
          "FLD_AR_ID",
          "STUDY_TYP",
          "FLD_ZONE",
          "ZONE_SUBTY",
          "SFHA_TF",
          "STATIC_BFE",
          "V_DATUM",
          "DEPTH",
          "LEN_UNIT",
          "VELOCITY",
          "VEL_UNIT",
          "AR_REVERT",
          "AR_SUBTRV",
          "BFE_REVERT",
          "DEP_REVERT",
          "DUAL_ZONE",
          "GFID",
          "SHAPE_LENG",
        ],
        extra: ["BLE_ID", "FP_AR_ID", "SHAPE_LENGTH"],
      },
      { missing, extra }
    )
  ) {
    throw new Error("INVARIANT BROKEN: Unexpected fp_pct_layers schema.");
  }

  return { missing, extra };
}

function getTempGPKGInfoForGDB(gdb) {
  const gdb_path = join(extracted_dir, gdb);

  const name = basename(gdb)
    .replace(/\//g, "_")
    .replace(/\.gdb$/, "");

  console.log(`\ngdb_path: ${gdb_path}; name: ${name}`);

  const gpkg_name = `${name}.gpkg`;
  const gpkg_path = join(merge_work_dir, gpkg_name);

  return { name, gpkg_name, gpkg_path };
}

// We create the tmp GPKGs so we can extract, then merge, only the S_Fld_Haz_Ar layers.
// Apparently ogrmerge.py does not support specifying specific layers.
function createTmpGPKGs(gdbs_with_fp_pct_layer) {
  // console.log(JSON.stringify({ gdbs_with_fp_pct_layer }, null, 4));

  mkdirSync(merged_fp_pct_dir, { recursive: true });

  if (existsSync(merge_work_dir)) {
    rmSync(merge_work_dir, { recursive: true });
  }

  mkdirSync(merge_work_dir, { recursive: true });

  const gpkg_paths = [];

  for (const gdb of gdbs_with_fp_pct_layer) {
    const gdb_path = join(extracted_dir, gdb);

    const { name, gpkg_path } = getTempGPKGInfoForGDB(gdb);

    if (existsSync(gpkg_path)) {
      throw new Error(`INVARIANT BROKEN: Dataset name collision ${name}.`);
    }

    // For WNY_Spatial_Files_3/04120104/BLE_04120104.gdb, the original SRS is incorrect.
    // See ../Projection_Fix.BLE_04120104.md for details.
    // TODO: Make sure currently set to California to support running on resubmitted input data.
    const a_srs =
      gdb === "WNY_Spatial_Files_3/04120104/BLE_04120104.gdb"
        ? "-a_srs EPSG:6541"
        : "";

    const create_cmd = `ogr2ogr -preserve_fid ${a_srs} -F 'GPKG' ${gpkg_path} ${gdb_path} ${fp_pct_layer_names.join(
      " "
    )}`;

    execSync(create_cmd);

    gpkg_paths.push(gpkg_path);
  }

  return gpkg_paths;
}

/*
function createGFIDField(gdbs_with_fp_pct_layer, schema_comparison) {
  for (const gdb of gdbs_with_fp_pct_layer) {
    const { gpkg_path } = getTempGPKGInfoForGDB(gdb);

    for (const layer_name of fp_pct_layer_names) {
      const alter_stmt = `ALTER TABLE ${layer_name} ADD COLUMN GFID TEXT;`;
      const alter_cmd = `ogrinfo -dialect SQLite -sql '${alter_stmt}' ${gpkg_path}`;

      execSync(alter_cmd);

      console.log(JSON.stringify({ extra: schema_comparison.extra }, null, 4));

      // NOTE: Without using json_patch, SQLite throws "too many arguments on function json_object"
      const json_object_args = schema_comparison.extra
        .filter((fld) => !/^SHAPE_LENGTH$/.test(fld))
        .map((field) => `'${field}',${field}`);

      console.log(JSON.stringify({ json_object_args }, null, 4));

      const update_stmt = `
        UPDATE ${layer_name}
          SET GFID = json_object(
            'meta',         json_object(
                              'type',   'BLE::${layer_name}',
                              'file',   '${gdb}',
                              'layer',  '${layer_name}',
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
}
*/

function createGFIDField(gdbs_with_fp_pct_layer) {
  for (const gdb of gdbs_with_fp_pct_layer) {
    const { gpkg_path } = getTempGPKGInfoForGDB(gdb);

    for (const layer_name of fp_pct_layer_names) {
      const alter_stmt = `ALTER TABLE ${layer_name} ADD COLUMN GFID TEXT;`;
      const alter_cmd = `ogrinfo -dialect SQLite -sql '${alter_stmt}' ${gpkg_path}`;

      execSync(alter_cmd);

      const update_stmt = `UPDATE ${layer_name} SET GFID = ( 'BLE:${gdb}:${layer_name}:' || ROWID )`;

      const update_cmd = `ogrinfo -dialect SQLite -sql "${update_stmt}" ${gpkg_path}`;

      execSync(update_cmd);
    }
  }
}

function createMergedGPKG(gpkg_paths) {
  // console.log(JSON.stringify({ gdbs_with_fp_pct_layer }, null, 4));

  if (existsSync(merged_gpkg)) {
    rmSync(merged_gpkg);
  }

  const input = gpkg_paths.join(" ");

  const cmd = `ogrmerge.py -single -f 'GPKG' -o ${merged_gpkg} ${input} -nln 'merged_ble_fp_pcts'`;

  execSync(cmd);

  return merged_gpkg;
}

function verifySrsProjectionUnitsIsFeet() {
  const cmd = `
    docker \\
      run \\
      --rm \\
      -v ${merged_fp_pct_dir}:/data \\
      avail/gis-analysis \\
      /bin/sh -c 'ogrinfo -json -wkt_format WKT1 -al -so /data/merged_ble_fp_pcts.gpkg merged_ble_fp_pcts'
    `;

  const meta_str = execSync(cmd);

  const meta = JSON.parse(meta_str);

  console.log(meta.layers[0].geometryFields[0].coordinateSystem.projjson.name);

  const projjson_name =
    meta.layers[0].geometryFields[0].coordinateSystem.projjson.name;

  if (!/^NAD83.*\(ftUS\)$/.test(projjson_name)) {
    throw new Error(
      "INVARIANT BROKEN: SRS expected to be NAD83 with ftUS units."
    );
  }

  return projjson_name;
}

function createMergedGDB() {
  const dir = join(__dirname, "..");

  console.log(dir);

  const cmd = `docker run --rm -v ${dir}:/data avail/gis-analysis /data/src/create_merged_fp_pct_gdb.sh`;

  console.log(cmd);
  const result = execSync(cmd);

  console.log(result.toString());
}

function dropExtraColumn() {
  const alter_stmt = "ALTER TABLE merged_ble_fp_pcts DROP COLUMN BLE_ID";

  const cmd = `
    docker \\
      run \\
      --rm \\
      -v ${merged_fp_pct_dir}:/data \\
      avail/gis-analysis \\
      /bin/sh -c "ogrinfo -sql '${alter_stmt}' /data/merged_ble_fp_pcts.gdb"
  `;

  execSync(cmd);
}

function addMissingColumns(missing) {
  const to_add = _.difference(missing, [
    "OGC_FID",
    "FLD_AR_ID",
    "GFID",
    "SHAPE_LENG",
  ]);

  for (const field of to_add) {
    const field_type = gdb_fields[field.toLowerCase()];

    const alter_stmt = `ALTER TABLE merged_ble_fp_pcts ADD COLUMN ${field} ${field_type}`;

    const cmd = `
      docker \\
        run \\
        --rm \\
        -v ${merged_fp_pct_dir}:/data \\
        avail/gis-analysis \\
        /bin/sh -c "ogrinfo -sql '${alter_stmt}' /data/merged_ble_fp_pcts.gdb"
    `;

    execSync(cmd);
    console.log("Added", field, "to merged_ble_fp_pcts.gdb");
  }
}

async function loadMergedGDB() {
  const creds = getPostgresConnectionString(pg_env);

  const gdb_path = join(__dirname, "../merged/fp_pct/merged_ble_fp_pcts.gdb");

  await dama_db.query(
    `
      CREATE SCHEMA IF NOT EXISTS floodplains ;
      DROP TABLE IF EXISTS floodplains.merged_ble_fp_pcts ;
    `,
    pg_env
  );

  const cmd = `
    ogr2ogr \\
      -F PostgreSQL \\
      PG:"${creds}" \\
      ${gdb_path} \\
      -nln 'floodplains.merged_ble_fp_pcts'
  `;

  execSync(cmd);
  console.log("loaded merged_ble_fp_pcts into Postgres db");
}

async function main() {
  const gdbs_with_fp_pct_layer = getBLEGeoDatabases();

  const fp_pct_layer_schema = getFpPctLayerSchema(gdbs_with_fp_pct_layer);

  const schema_comparison =
    compareFpPctLayerSchemaToRequired(fp_pct_layer_schema);

  const gpkg_paths = createTmpGPKGs(gdbs_with_fp_pct_layer);
  createGFIDField(gdbs_with_fp_pct_layer);

  createMergedGPKG(gpkg_paths);

  verifySrsProjectionUnitsIsFeet();
  createMergedGDB();

  dropExtraColumn();
  addMissingColumns(schema_comparison.missing);

  await loadMergedGDB();
}

// getSourceWithLayerNamesSummary();
main();
