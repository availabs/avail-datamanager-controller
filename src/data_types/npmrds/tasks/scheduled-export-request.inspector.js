#!/usr/bin/env node

// This script is included in the repo as an example of how to debug UI Changes to the MDD.

require("ts-node").register();
require("tsconfig-paths").register();

const { runInDamaContext } = require("../../../data_manager/contexts/index.ts");

const { getBatchExportRequest } = require("./scheduled-etl.ts");

const source_id = 93;

const etl_context = {
  meta: {
    pgEnv: "npmrds",
    etl_context_id: -1,
  },
};

async function main() {
  await runInDamaContext(etl_context, async () => {
    try {
      const export_request = await getBatchExportRequest(
        source_id,
        "ny",
        "2024-05-05"
      );

      console.log(export_request);
    } catch (err) {
      console.error(err);
    }
  });
}

main();
