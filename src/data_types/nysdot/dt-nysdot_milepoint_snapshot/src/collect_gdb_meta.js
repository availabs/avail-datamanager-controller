#!/usr/bin/env node

// To use the ogrinfo -json flag, needed to run this script
//   in a docker container with ogrinfo version GDAL 3.7.2, released 2023/09/05.

const { execSync } = require("child_process");
const { writeFileSync, unlinkSync, existsSync } = require("fs");
const { join, relative, dirname } = require("path");

const { globSync } = require("glob");

const extracts_dir = join(__dirname, "../initial_data/extracted");

const meta_file = join(__dirname, "../gdbs_meta.json");

if (existsSync(meta_file)) {
  unlinkSync(meta_file);
}

const gdb_dirs = globSync("**/*.gdb/", {
  cwd: join(__dirname, "../initial_data/extracted"),
  mark: true,
  withFileTypes: true,
})
  .filter((data) => data.isDirectory())
  .map((data) => data.fullpath());


// const shp_dirs = globSync("**/*.shp", {
//   cwd: join(__dirname, "../initial_data/extracted"),
//   mark: true,
//   withFileTypes: true,
// })
//   .filter((data) => data.isFile())
//   .map((data) => dirname(data.fullpath()));
//
// const dataset_dirs = [...gdb_dirs, ...shp_dirs];

const dataset_dirs = [...gdb_dirs].sort();

console.log(JSON.stringify(dataset_dirs, null, 4));

const gdb_meta = {};

for (const dset of dataset_dirs) {
  try {
    const id = relative(extracts_dir, dset);

    const meta_str = execSync(`ogrinfo -json -al -so ${dset} | jq -c .`, { encoding: 'utf8' }).toString();

    gdb_meta[id] = JSON.parse(meta_str);
  } catch (err) {
    console.error(err);
  }
}

writeFileSync(meta_file, JSON.stringify(gdb_meta, null, 4));
