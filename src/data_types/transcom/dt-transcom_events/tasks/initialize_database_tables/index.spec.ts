import dama_db from "data_manager/dama_db";
import { runInDamaContext } from "data_manager/contexts";

import main from ".";

const PG_ENV = "ephemeral_test_db";

test("initializes transcom and _transcom_admin schemas", async () => {
  await dama_db.query(
    [
      "BEGIN ;",
      "DROP SCHEMA IF EXISTS transcom CASCADE ;",
      "DROP SCHEMA IF EXISTS _transcom_admin CASCADE ;",
      "COMMIT ;",
    ],
    PG_ENV
  );

  const ctx = { meta: { pgEnv: PG_ENV } };

  await runInDamaContext(ctx, main);

  const { rows: main_schema_tables_result } = await dama_db.query(
    `
      SELECT
          table_name
        FROM information_schema.tables
        WHERE ( table_schema = 'transcom' )
    `,
    PG_ENV
  );

  const main_schema_tables = main_schema_tables_result.map(
    ({ table_name }) => table_name
  );

  expect(main_schema_tables).toContain("transcom_events_aggregate");
  expect(main_schema_tables).toContain("nysdot_transcom_event_classifications");

  const { rows: admin_schema_tables_result } = await dama_db.query(
    `
      SELECT
          table_name
        FROM information_schema.tables
        WHERE ( table_schema = '_transcom_admin' )
    `,
    PG_ENV
  );

  const admin_schema_tables = admin_schema_tables_result.map(
    ({ table_name }) => table_name
  );

  expect(admin_schema_tables).toContain(
    "transcom_event_administative_geographies"
  );
  expect(admin_schema_tables).toContain("transcom_event_congestion_data");
  expect(admin_schema_tables).toContain("transcom_events_expanded");
  expect(admin_schema_tables).toContain("transcom_events_expanded_view");
});
