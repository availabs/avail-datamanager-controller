import _ from "lodash";
import dedent from "dedent";
import pgFormat from "pg-format";
import { DateTime } from "luxon";

import dama_db from "../../src/data_manager/dama_db";
import dama_meta from "../../src/data_manager/meta";

import { stateAbbr2FipsCode } from "../../src/data_utils/constants/stateFipsCodes";

import {
  NpmrdsDataSources,
  NpmrdsDatabaseSchemas,
} from "../../src/data_types/npmrds/domain";

import { initializeDamaSources } from "../../src/data_types/npmrds/utils/dama_sources";
// import makeTravelTimesExportTablesAuthoritative from "../../src/data_types/npmrds/dt-npmrds_travel_times/actions/makeTravelTimesExportTablesAuthoritative";

import { PG_ENV, InheritanceTree, state_re } from "./domain";

import {
  parseLeafTableInfo,
  getNpmrdsTablesInheritanceTree,
  getRootTable,
  getLeafTables,
} from "./utils/npmrds_travel_times";

const npmrds_travel_times_schema = NpmrdsDatabaseSchemas.NpmrdsTravelTimes;
const npmrds_travel_times_imports_schema =
  NpmrdsDatabaseSchemas.NpmrdsTravelTimesImports;

async function getSummaryStats(inheritance_tree: InheritanceTree) {
  const { root_table_schema, root_table_name } = getRootTable(inheritance_tree);

  const sql = dedent(
    pgFormat(
      `
        SELECT
            state,
            COUNT(1) AS row_count
          FROM %I.%I
          GROUP BY state
          ORDER BY state
      `,
      root_table_schema,
      root_table_name
    )
  );

  const { rows: stats } = await dama_db.query(sql, PG_ENV);

  return stats;
}

async function noInheritAllChildTables(inheritance_tree: InheritanceTree) {
  for (const {
    parent_table_schema,
    parent_table_name,
    table_schema,
    table_name,
  } of inheritance_tree) {
    if (parent_table_name) {
      const sql = dedent(
        pgFormat(
          `
            ALTER TABLE %I.%I
              NO INHERIT %I.%I
          `,
          table_schema,
          table_name,
          parent_table_schema,
          parent_table_name
        )
      );

      await dama_db.query(sql);
    }
  }
}

async function redefineRootTable(inheritance_tree: InheritanceTree) {
  const { root_table_schema, root_table_name } = getRootTable(inheritance_tree);

  if (root_table_schema !== "public") {
    throw new Error(
      `root_table_schema should be "public" but got ${root_table_schema}`
    );
  }

  if (root_table_name !== "npmrds") {
    throw new Error(
      `root_table_name should be "npmrds" but got ${root_table_name}`
    );
  }

  const sql = dedent(
    `
      DROP TABLE public.npmrds ; -- NOTE: Will fail if still has child tables.

      CREATE TABLE public.npmrds (
        tmc                               VARCHAR(9),
        date                              DATE,
        epoch                             SMALLINT,
        travel_time_all_vehicles          REAL,
        travel_time_passenger_vehicles    REAL,
        travel_time_freight_trucks        REAL,
        data_density_all_vehicles         CHAR,
        data_density_passenger_vehicles   CHAR,
        data_density_freight_trucks       CHAR,
        state                             CHAR(2) NOT NULL
      )
        PARTITION BY LIST (state)
      ;
    `
  );

  await dama_db.query(sql);
}

async function redefineStateRootTables(inheritance_tree: InheritanceTree) {
  const { root_table_schema, root_table_name } = getRootTable(inheritance_tree);

  const state_root_tables = inheritance_tree.filter(({ depth }) => depth === 1);

  const invariant_violators = state_root_tables.filter(
    ({ parent_table_schema, parent_table_name, table_schema, table_name }) =>
      parent_table_schema !== root_table_schema ||
      parent_table_name !== root_table_name ||
      table_name !== "npmrds" ||
      !state_re.test(table_schema)
  );

  if (invariant_violators.length) {
    const invariant_violator_tables = invariant_violators.map(
      ({ table_schema, table_name }) =>
        pgFormat("%I.%I", table_schema, table_name)
    );

    console.log(
      JSON.stringify(
        { root_table_schema, root_table_name, invariant_violators },
        null,
        4
      )
    );

    throw Error(
      `The following table violated the state root invariants: ${invariant_violator_tables}`
    );
  }

  for (const { table_schema } of state_root_tables) {
    const sql = dedent(
      pgFormat(
        `
          DROP TABLE %I.npmrds ; -- NOTE: Will fail if has child tables since no CASCADE.

          CREATE TABLE %I.npmrds
            PARTITION OF public.npmrds
            FOR VALUES IN (%L)
            PARTITION BY RANGE (date)
          ;
        `,
        table_schema,
        table_schema,
        table_schema
      )
    );

    await dama_db.query(sql);
  }
}

async function createStateYearMonthTables(inheritance_tree: InheritanceTree) {
  await dama_db.query(
    `CREATE SCHEMA IF NOT EXISTS ${npmrds_travel_times_schema}`
  );

  await dama_db.query(
    `CREATE SCHEMA IF NOT EXISTS ${npmrds_travel_times_imports_schema}`
  );

  await initializeDamaSources();

  const dama_source_id_sql = dedent(`
    SELECT
        source_id
      FROM data_manager.sources
      WHERE ( name = $1 )
  `);

  const q = {
    text: dama_source_id_sql,
    values: [NpmrdsDataSources.NpmrdsTravelTimesImports],
  };

  console.log(JSON.stringify({ q }, null, 4));

  const {
    rows: [{ source_id: npmrds_travel_times_imports_source_id }],
  } = await dama_db.query(q);

  const view_exists_sql = dedent(`
    SELECT EXISTS (
      SELECT
          1
        FROM data_manager.views
        WHERE (
          ( source_id = $1 )
          AND
          ( table_schema = $2 )
          AND
          ( table_name = $2 )
        )
    ) AS view_exists
  `);

  const leaf_tables = getLeafTables(inheritance_tree);

  // const new_view_ids: number[] = [];

  for (const table_info of leaf_tables) {
    const { table_schema, table_name } = table_info;
    const { state, start_date, end_date } = parseLeafTableInfo(table_info);

    const [yyyy, mm] = start_date.split("-");

    const year = +yyyy;

    const state_year_table_name = `npmrds_${state}_${yyyy}`;
    const state_yrmo_table_name = `npmrds_${state}_${yyyy}${mm}`;

    const state_year_sql = dedent(
      pgFormat(
        `
          CREATE TABLE IF NOT EXISTS %I.%I
            PARTITION OF %I.npmrds
            FOR VALUES FROM (%L) TO (%L)
            PARTITION BY RANGE (date)
          ;
        `,
        npmrds_travel_times_schema,
        state_year_table_name,
        state,
        `${year}-01-01`,
        `${year + 1}-01-01`
      )
    );

    await dama_db.query(state_year_sql);

    const state_yrmo_sql = dedent(
      pgFormat(
        `
          CREATE TABLE IF NOT EXISTS %I.%I
            PARTITION OF %I.%I
            FOR VALUES FROM (%L) TO (%L)
            PARTITION BY RANGE (date)
          ;
        `,
        npmrds_travel_times_schema,
        state_yrmo_table_name,
        npmrds_travel_times_schema,
        state_year_table_name,
        start_date,
        end_date
      )
    );

    await dama_db.query(state_yrmo_sql);

    const start = start_date.replace(/-/g, "");

    const end_date_inclusive = DateTime.fromISO(end_date)
      .minus({ day: 1 })
      .toISODate();

    const end = end_date_inclusive.replace(/-/g, "");

    const new_table_name = `npmrdsx_${state}_from_${start}_to_${end}_v00000000t000000`;

    // rename primary key

    const move_leaf_table_sql = dedent(
      pgFormat(
        `
          ALTER TABLE %I.%I
            SET SCHEMA %I
          ;

          ALTER TABLE %I.%I
            RENAME TO %I
        `,
        table_schema,
        table_name,
        npmrds_travel_times_imports_schema,
        npmrds_travel_times_imports_schema,
        table_name,
        new_table_name
      )
    );

    await dama_db.query(move_leaf_table_sql);

    const make_state_col_not_null = dedent(
      pgFormat(
        `
          ALTER TABLE %I.%I
            ALTER COLUMN state SET NOT NULL
          ;
        `,
        npmrds_travel_times_imports_schema,
        new_table_name
      )
    );

    await dama_db.query(make_state_col_not_null);

    //  There's data gaps for ct, pa, on, and qc.
    //    Therefore we cannot use makeTravelTimesExportTablesAuthoritative for those.
    // if (state !== "ny" && state !== "nj") {
    const attach_leaf_table_sql = dedent(
      pgFormat(
        `
          ALTER TABLE %I.%I
            ATTACH PARTITION %I.%I
              FOR VALUES FROM (%L) TO (%L)
          ;
        `,
        npmrds_travel_times_schema,
        state_yrmo_table_name,
        npmrds_travel_times_imports_schema,
        new_table_name,
        start_date,
        end_date
      )
    );

    await dama_db.query(attach_leaf_table_sql);
    // }

    // https://dba.stackexchange.com/a/214877
    const constraints_sql = dedent(
      pgFormat(
        `
          SELECT
              con.conname,
              con.contype
            FROM pg_catalog.pg_constraint AS con
              INNER JOIN pg_catalog.pg_class rel
                  ON ( rel.oid = con.conrelid )
              INNER JOIN pg_catalog.pg_namespace nsp
                  ON ( nsp.oid = connamespace )
            WHERE (
              ( nsp.nspname = %L )
              AND
              ( rel.relname = %L )
            )
          ;
        `,
        npmrds_travel_times_imports_schema,
        new_table_name
      )
    );

    const { rows: check_constraints } = await dama_db.query(constraints_sql);

    for (const { conname, contype } of check_constraints) {
      // RENAME the PRIMARY KEY so it would match default name.
      if (contype === "p") {
        const rename_primary_key_sql = dedent(
          pgFormat(
            `
              ALTER INDEX %I.%I
                RENAME TO %I
              ;
            `,
            npmrds_travel_times_imports_schema,
            conname,
            `${new_table_name}_pkey`
          )
        );

        await dama_db.query(rename_primary_key_sql);
      }

      // DROP CHECK CONSTRAINTs because they are redundant with PARTITIONed tables.
      if (contype === "c") {
        const drop_constraint_sql = dedent(
          pgFormat(
            `
              ALTER TABLE %I.%I
                DROP CONSTRAINT %I
              ;
            `,
            npmrds_travel_times_imports_schema,
            new_table_name,
            conname
          )
        );

        await dama_db.query(drop_constraint_sql);
      }
    }

    const {
      rows: [{ view_exists }],
    } = await dama_db.query({
      text: view_exists_sql,
      values: [npmrds_travel_times_imports_source_id, table_schema, table_name],
    });

    if (view_exists) {
      const table_full_name = pgFormat("%I.%I", table_schema, table_name);

      throw new Error(
        `dama_manager.view already exists for ${table_full_name}`
      );
    }

    const geography_version = stateAbbr2FipsCode[state];

    const view_meta = {
      source_id: npmrds_travel_times_imports_source_id,
      interval_version: `${start}-${end}`,
      geography_version,
      version: table_name,
      table_schema: npmrds_travel_times_imports_schema,
      table_name: new_table_name,
      start_date,
      end_date: end_date_inclusive,
      last_updated: "1970-01-01",
      metadata: {
        name: new_table_name,
        year,
        state,
        start_date,
        end_date: end_date_inclusive,
        table_name: new_table_name,
        is_expanded: 1,
        is_complete_week: false,
        is_complete_month: true,
      },
    };

    // const { view_id } = await dama_meta.createNewDamaView(view_meta);
    await dama_meta.createNewDamaView(view_meta);

    // if (state === "ny" || state === "nj") {
    // new_view_ids.push(view_id);
    // }
  }

  // await makeTravelTimesExportTablesAuthoritative(new_view_ids);
}

async function main() {
  dama_db.runInTransactionContext(async () => {
    const inheritance_tree = await getNpmrdsTablesInheritanceTree();

    const before_stats = await getSummaryStats(inheritance_tree);

    await noInheritAllChildTables(inheritance_tree);

    await redefineRootTable(inheritance_tree);

    await redefineStateRootTables(inheritance_tree);

    await createStateYearMonthTables(inheritance_tree);

    const after_stats = await getSummaryStats(inheritance_tree);

    console.log(JSON.stringify({ before_stats, after_stats }, null, 4));

    if (!_.isEqual(before_stats, after_stats)) {
      throw new Error(`before_stats !== after_stats`);
    }
  }, PG_ENV);
}

main();
