import _ from "lodash";

import dama_db from "data_manager/dama_db";
import initializeForYear from "./initialize";
import createNetworkNodeConformalMatchingFunctions from "./createNetworkNodeConformalMatchingFunctions";

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

  await dama_db.runInTransactionContext(
    createNetworkNodeConformalMatchingFunctions,
    PG_ENV
  );
}

main();
