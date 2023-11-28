#!/usr/bin/env node

const gdbs_meta = require("../gdbs_meta.json");

const gdbs = Object.keys(gdbs_meta);

const summary = {};

for (const gdb of gdbs) {
  const { layers } = gdbs_meta[gdb];

  summary[gdb] = layers.map(({ name }) => name);
}

console.log(JSON.stringify(summary, null, 4));
