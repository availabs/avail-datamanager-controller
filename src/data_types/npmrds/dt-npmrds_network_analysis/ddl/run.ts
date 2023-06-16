import _ from "lodash";

import dama_db from "data_manager/dama_db";
import initializeForYear from "./initialize";
import conformalMatching from "./conformalMatching";
import createTmcSimilarityTables from "./createTmcSimilarityTables";

const PG_ENV = "dama_dev_1";

const years = _.range(2017, 2023);

async function main() {
  await Promise.all(
    years.map((year) =>
      dama_db.runInTransactionContext(async () => {
        await initializeForYear(year);
      }, PG_ENV)
    )
  );

  await Promise.all([
    dama_db.runInTransactionContext(createTmcSimilarityTables, PG_ENV),
    dama_db.runInTransactionContext(conformalMatching, PG_ENV),
  ]);
}

main();
