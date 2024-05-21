#!/usr/bin/env node

require("ts-node").register();
require("tsconfig-paths").register();

const {
  runInDamaContext,
} = require("../../../../data_manager/contexts/index.ts");

const { default: doQA } = require("./qa_imports.ts");

// const view_ids = [3028, 3032, 3036, 3040, 3044, 3048];
// const view_ids = [3004, 3008, 3012, 3016];
const view_ids = [3004,3008,3012,3016,3020,3024,3028,3032]

const etl_context = {
  meta: {
    pgEnv: "npmrds",
    etl_context_id: -1,
  },
};

async function main() {
  await runInDamaContext(etl_context, async () => {
    try {
      const qa_stats = await doQA(view_ids);

      // console.log(JSON.stringify(qa_stats, null, 4));
      const make_auth = view_ids.map(id => `--dama_view_ids ${id}`).join(' ')

      console.log(make_auth)

    } catch (err) {
      console.error(err);
    }
  });
}

main();
