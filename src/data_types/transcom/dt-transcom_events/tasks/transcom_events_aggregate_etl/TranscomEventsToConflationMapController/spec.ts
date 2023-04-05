import { inspect } from "util";

import { QueryResult } from "pg";
import TranscomEventsToConflationMapController from ".";

import { getConnectedPgClient } from "../../../utils/PostgreSQL";

const PG_ENV = "development";

async function main() {
  const db = await getConnectedPgClient(PG_ENV);

  try {
    db.on("notice", (msg) => console.log("notice", msg.message));

    const ctrl = new TranscomEventsToConflationMapController(db, 0);

    // @ts-ignore
    let result: QueryResult;

    // @ts-ignore
    // result = await ctrl.updateTranscomEventsOntoRoadNetwork();
    // @ts-ignore
    result = await ctrl.updateTranscomEventsByTmcSummary();
    console.log(inspect(result));
  } catch (err) {
    console.error(err);
  } finally {
    await db.end();
  }
}

main();
