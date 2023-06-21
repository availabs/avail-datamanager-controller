import _ from "lodash";

import dama_db from "data_manager/dama_db";
import initializeForYear from "./initialize";
import initializeConformalMatching from "./conformalMatching/initializeConformalMatching";
import conformalMatching from "./conformalMatching";

const PG_ENV = "dama_dev_1";

// exclusive on max
const years = _.range(2017, 2023);

async function main() {
  await Promise.all(
    years.map((year) =>
      dama_db.runInTransactionContext(async () => {
        await initializeForYear(year);
      }, PG_ENV)
    )
  );

  const year_pairs = years.reduce(
    (acc: Array<[number, number]>, year: number, i) => {
      for (const following_year of years.slice(i + 1)) {
        acc.push([year, following_year]);
      }

      return acc;
    },
    [] as Array<[number, number]>
  );

  await dama_db.runInTransactionContext(initializeConformalMatching, PG_ENV);

  await Promise.all(
    year_pairs.map((year_pair) =>
      dama_db.runInTransactionContext(
        () => conformalMatching(...year_pair),
        PG_ENV
      )
    )
  );
}

main();
