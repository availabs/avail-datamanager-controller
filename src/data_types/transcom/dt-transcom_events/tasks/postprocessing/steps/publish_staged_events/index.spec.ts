import { copyFileSync, symlinkSync, readdirSync } from "fs";
import { join } from "path";

import tmp from "tmp";

// import dama_db from "data_manager/dama_db";
import dama_events from "data_manager/events";

import { runInDamaContext } from "data_manager/contexts";

import etl_dir from "constants/etlDir";

import main from ".";
import getEtlWorkDir, { getEtlWorkDirMeta } from "../utils/etlWorkDir";
import getEtlContextLocalStateSqliteDb from "../utils/getEtlContextLocalStateSqliteDb";
import getPostgresStagingSchemaName from "../utils/getPostgresStagingSchemaName";

const test_data_dir = join(
  __dirname,
  "../../test_data/dt-transcom_events.ephemeral_test_db.-1"
);

const PG_ENV = "ephemeral_test_db";

test("copies staged TRANSCOM events to the published table", async () => {
  throw new Error("TODO: Implmement test");
});
