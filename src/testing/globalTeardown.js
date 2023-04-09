require("tsconfig-paths").register();

const { default: dama_db } = require("../data_manager/dama_db");

module.exports = async function globalTeardown() {
  await dama_db.shutdown();
};
