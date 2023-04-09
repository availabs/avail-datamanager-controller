import { Client } from "pg";

import { getNodePgCredentials } from "../data_manager/dama_db/postgres/PostgreSQL";

const PG_ENV = "test_db";

export default async function globalSetup() {
  console.log(
    "\nRunning Jest globalSetup. DROPPING and CREATING test_db DATABASE."
  );

  const creds = getNodePgCredentials(PG_ENV);

  if (
    creds.host !== "127.0.0.1" ||
    creds.database !== "test_db" ||
    creds.user !== "dama_test_user"
  ) {
    console.error(
      "ERROR: The postgres.test_db.env file has been modified. Abandining all tests."
    );
    process.exit(1);
  }

  creds.database = "postgres";

  const db = new Client(creds);

  try {
    await db.connect();

    await db.query("DROP DATABASE IF EXISTS test_db;");
    await db.query("CREATE DATABASE test_db;");
  } catch (err) {
    throw err;
  } finally {
    await db.end();
  }
}
