require("tsconfig-paths").register();
// https://github.com/dividab/tsconfig-paths

const { default: dama_db } = require("../data_manager/dama_db");
const { default: dama_events } = require("../data_manager/events");

module.exports = async function globalTeardown() {
  console.log("Shutting down dama_db");
  await dama_db.shutdown();
  console.log("Shut down dama_db");

  console.log("Shutting down dama_events");
  await dama_events.shutdown();
  console.log("Shut down dama_events");
};
