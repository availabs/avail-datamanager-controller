import dedent from "dedent";
import pgFormat from "pg-format";

import dama_db from "../../src/data_manager/dama_db";

import { TableInfo } from "./domain";

import {
  parseLeafTableInfo,
  getNpmrdsTablesInheritanceTree,
  getLeafTables,
} from "./utils";

const PG_ENV = "dama_dev_1";

const leaf_table_name_re = /^npmrds_y\d{4}m\d{2}$/;

async function loadMockData(table_info: TableInfo) {
  const { table_schema, table_name } = table_info;

  const table_full_name = pgFormat("%I.%I", table_schema, table_name);

  if (!leaf_table_name_re.test(table_name)) {
    return;
  }

  const { start_date } = parseLeafTableInfo(table_info);

  const tmc = `0${table_schema}-00000`;

  const sql = dedent(
    pgFormat(
      `
        DELETE FROM %I.%I
          WHERE ( tmc = %L )
        ;

        INSERT INTO %I.%I
          SELECT
              %L AS tmc,
              %L AS date,
              t.epoch,
              t.epoch AS travel_time_all_vehicles
            FROM generate_series(0, 287) AS t(epoch)
        ;
      `,
      table_schema,
      table_name,
      tmc,
      table_schema,
      table_name,
      tmc,
      start_date
    )
  );

  // @ts-ignore
  const [, { rowCount }] = await dama_db.query(sql, PG_ENV);

  console.log(`inserted ${rowCount} rows into ${table_full_name}`);

  await dama_db.query(sql);
}

async function main() {
  await dama_db.runInTransactionContext(async () => {
    const inheritance_tree = await getNpmrdsTablesInheritanceTree();

    const leaf_tables = getLeafTables(inheritance_tree);

    for (const table_info of leaf_tables) {
      await loadMockData(table_info);
    }
  }, PG_ENV);
}

main();
