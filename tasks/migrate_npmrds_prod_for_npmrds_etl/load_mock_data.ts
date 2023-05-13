import dedent from "dedent";
import pgFormat from "pg-format";

import dama_db from "../../src/data_manager/dama_db";

import { TableInfo } from "./domain";

import {
  getNpmrdsTablesInheritanceTree,
  getLeafTables as getNpmrdsTravelTimesLeafTables,
  parseLeafTableInfo as parseNpmrdsTravelTimesLeafTableInfo,
} from "./utils/npmrds_travel_times";

import {
  getTmcIdentificationInheritanceTreesByYear,
  getLeafTables as getTmcIdentificationLeafTables,
  parseLeafTableInfo as parseTmcIdentificationLeafTableInfo,
} from "./utils/tmc_identification";

const PG_ENV = "dama_dev_1";

async function loadMockTmcIdentificationData(table_info: TableInfo) {
  const { table_schema, table_name } = table_info;

  const leaf_table_name_re = /^tmc_identification_\d{4}_v\d{8}t\d{6}$/;

  if (!leaf_table_name_re.test(table_name)) {
    return;
  }

  const table_full_name = pgFormat("%I.%I", table_schema, table_name);

  const { state } = parseTmcIdentificationLeafTableInfo(table_info);

  for (let i = 0; i < 10; ++i) {
    const tmc = `0${state}-000${i}0`.slice(0, 9);

    const sql = dedent(
      pgFormat(
        `
          DELETE FROM %I.%I
            WHERE ( tmc = %L )
          ;

          INSERT INTO %I.%I (tmc, state, aadt)
            SELECT
                %L AS tmc,
                %L AS state,
                %s AS aadt
          ;
        `,
        table_schema,
        table_name,
        tmc,
        table_schema,
        table_name,
        tmc,
        state,
        Math.random() * 1e5
      )
    );

    // @ts-ignore
    await dama_db.query(sql);
  }
}

async function loadMockNpmrdsTravelTimesData(table_info: TableInfo) {
  const { table_schema, table_name } = table_info;

  const leaf_table_name_re = /^npmrds_y\d{4}m\d{2}$/;

  if (!leaf_table_name_re.test(table_name)) {
    return;
  }

  const table_full_name = pgFormat("%I.%I", table_schema, table_name);

  const { start_date } = parseNpmrdsTravelTimesLeafTableInfo(table_info);

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
    const tmc_identification_inheritance_trees_by_year =
      await getTmcIdentificationInheritanceTreesByYear();

    for (const year of Object.keys(
      tmc_identification_inheritance_trees_by_year
    )) {
      const inheritance_tree =
        tmc_identification_inheritance_trees_by_year[year];

      const tmc_identification_leaf_tables =
        getTmcIdentificationLeafTables(inheritance_tree);

      for (const table_info of tmc_identification_leaf_tables) {
        await loadMockTmcIdentificationData(table_info);
      }
    }

    const travel_times_inheritance_tree =
      await getNpmrdsTablesInheritanceTree();

    const travel_times_leaf_tables = getNpmrdsTravelTimesLeafTables(
      travel_times_inheritance_tree
    );

    for (const table_info of travel_times_leaf_tables) {
      await loadMockNpmrdsTravelTimesData(table_info);
    }
  }, PG_ENV);
}

main();
