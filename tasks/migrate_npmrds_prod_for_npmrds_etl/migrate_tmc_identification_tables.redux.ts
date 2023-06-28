// This script fixes the post-migration tmc_identification table definitions.
// It removes the PRIMARY KEYs from the parent tables.
// This allows the PRIMARY KEY on the leaf tables to be (tmc) rather than (tmc, state).

import _ from "lodash";
import dedent from "dedent";
import pgFormat from "pg-format";

import dama_db from "../../src/data_manager/dama_db";

import { PG_ENV, state_re } from "./domain";

import {
  InheritanceTreesByYear,
  parseLeafTableInfo,
  getTmcIdentificationInheritanceTreesByYear,
  getRootTable,
  getLeafTables,
} from "./utils/tmc_identification_migrated";

const root_table_re = /^tmc_identification_\d{4}$/;

function getSummaryStats(inheritance_trees_by_year: InheritanceTreesByYear) {
  const years = Object.keys(inheritance_trees_by_year).sort();

  return Promise.all(
    years.map(async (year) => {
      const { root_table_schema, root_table_name } = getRootTable(
        inheritance_trees_by_year[year]
      );

      const sql = dedent(
        pgFormat(
          `
            SELECT
                state,
                %L AS year,
                COUNT(1) AS row_count,
                MD5(string_agg(aadt::TEXT, '|' ORDER BY tmc)) AS aadt_md5sum
              FROM %I.%I
              GROUP BY state
              ORDER BY state
          `,
          year,
          root_table_schema,
          root_table_name
        )
      );

      const { rows: stats } = await dama_db.query(sql, PG_ENV);

      return stats;
    })
  );
}

async function noInheritAllChildTables(
  inheritance_trees_by_year: InheritanceTreesByYear
) {
  const years = Object.keys(inheritance_trees_by_year).sort();

  for (const year of years) {
    for (const {
      parent_table_schema,
      parent_table_name,
      table_schema,
      table_name,
    } of inheritance_trees_by_year[year]) {
      if (parent_table_name) {
        const sql = dedent(
          pgFormat(
            `
              ALTER TABLE %I.%I
                DETACH PARTITION %I.%I
              ;
            `,
            parent_table_schema,
            parent_table_name,
            table_schema,
            table_name
          )
        );

        await dama_db.query(sql);
      }
    }
  }
}

async function redefineRootTables(
  inheritance_trees_by_year: InheritanceTreesByYear
) {
  const years = Object.keys(inheritance_trees_by_year).sort();

  for (const year of years) {
    const inheritance_tree = inheritance_trees_by_year[year];

    const { root_table_schema, root_table_name } =
      getRootTable(inheritance_tree);

    if (root_table_schema !== "public") {
      throw new Error(
        `root_table_schema should be "public" but got ${root_table_schema}`
      );
    }

    if (!root_table_re.test(root_table_name)) {
      throw new Error(
        `root_table_name should match /^tmc_identification_\\d{4}$/ but got ${root_table_name}`
      );
    }

    const sql = dedent(
      pgFormat(
        `
          DROP TABLE public.%I ; -- NOTE: Will fail if has child tables.

          CREATE TABLE public.%I (
            tmc                      CHARACTER VARYING,
            type                     CHARACTER VARYING,
            road                     CHARACTER VARYING,
            road_order               REAL,
            intersection             CHARACTER VARYING,
            tmclinear                INTEGER,
            country                  CHARACTER VARYING,
            state                    CHARACTER VARYING,
            county                   CHARACTER VARYING,
            zip                      CHARACTER VARYING,
            direction                CHARACTER VARYING,
            start_latitude           DOUBLE PRECISION,
            start_longitude          DOUBLE PRECISION,
            end_latitude             DOUBLE PRECISION,
            end_longitude            DOUBLE PRECISION,
            miles                    DOUBLE PRECISION,
            frc                      SMALLINT,
            border_set               CHARACTER VARYING,
            isprimary                SMALLINT,
            f_system                 SMALLINT,
            urban_code               INTEGER,
            faciltype                SMALLINT,
            structype                SMALLINT,
            thrulanes                SMALLINT,
            route_numb               INTEGER,
            route_sign               SMALLINT,
            route_qual               SMALLINT,
            altrtename               CHARACTER VARYING,
            aadt                     INTEGER,
            aadt_singl               INTEGER,
            aadt_combi               INTEGER,
            nhs                      SMALLINT,
            nhs_pct                  SMALLINT,
            strhnt_typ               SMALLINT,
            strhnt_pct               SMALLINT,
            truck                    SMALLINT,
            timezone_name            CHARACTER VARYING,
            active_start_date        DATE,
            active_end_date          DATE,
            download_timestamp       TIMESTAMP
          )
            PARTITION BY LIST (state)
          ;
        `,
        root_table_name,
        root_table_name
      )
    );

    await dama_db.query(sql);
  }
}

async function redefineStateRootTables(
  inheritance_trees_by_year: InheritanceTreesByYear
) {
  const years = Object.keys(inheritance_trees_by_year).sort();

  for (const year of years) {
    const inheritance_tree = inheritance_trees_by_year[year];
    const { root_table_schema, root_table_name } =
      getRootTable(inheritance_tree);

    const state_root_tables = inheritance_tree.filter(
      ({ depth }) => depth === 1
    );

    const invariant_violators = state_root_tables.filter(
      ({ parent_table_schema, parent_table_name, table_schema, table_name }) =>
        parent_table_schema !== root_table_schema ||
        parent_table_name !== root_table_name ||
        !state_re.test(table_schema) ||
        !root_table_re.test(table_name)
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

    for (const { table_schema, table_name } of state_root_tables) {
      const sql = dedent(
        pgFormat(
          `
            DROP TABLE %I.%I ; -- NOTE: Will fail if has child tables since no CASCADE.

            -- NOTE: Parititioning again so table_name consistent while imports named using timestamp.
            CREATE TABLE %I.%I
              PARTITION OF public.%I
              FOR VALUES IN (%L)
              PARTITION BY LIST (state)
            ;
          `,
          table_schema,
          table_name,
          table_schema,
          table_name,
          table_name,
          table_schema.toUpperCase()
        )
      );

      await dama_db.query(sql);
    }
  }
}

async function alterLeafTablesPrimaryKeys(
  inheritance_trees_by_year: InheritanceTreesByYear
) {
  const years = Object.keys(inheritance_trees_by_year).sort();

  for (const yr of years) {
    const inheritance_tree = inheritance_trees_by_year[yr];

    const leaf_tables = getLeafTables(inheritance_tree);

    for (const table_info of leaf_tables) {
      const {
        parent_table_schema,
        parent_table_name,
        table_schema,
        table_name,
      } = table_info;

      const { state } = parseLeafTableInfo(table_info);

      const pkey_name = `${table_name}_pkey`;

      const alter_primary_key_sql = dedent(
        pgFormat(
          `
            ALTER TABLE %I.%I
              DROP CONSTRAINT %I
            ;

            ALTER TABLE %I.%I
              ADD CONSTRAINT %I PRIMARY KEY (tmc)
            ;

            CLUSTER %I.%I USING %I ;
          `,
          table_schema,
          table_name,
          pkey_name,

          table_schema,
          table_name,
          pkey_name,

          table_schema,
          table_name,
          pkey_name
        )
      );

      await dama_db.query(alter_primary_key_sql);

      const attach_leaf_table_sql = dedent(
        pgFormat(
          `
            ALTER TABLE %I.%I
              ATTACH PARTITION %I.%I
                FOR VALUES IN (%L)
            ;
          `,
          parent_table_schema,
          parent_table_name,
          table_schema,
          table_name,
          state.toUpperCase()
        )
      );

      await dama_db.query(attach_leaf_table_sql);
    }
  }
}

async function main() {
  dama_db.runInTransactionContext(async () => {
    const inheritance_trees_by_year =
      await getTmcIdentificationInheritanceTreesByYear();

    const before_stats = await getSummaryStats(inheritance_trees_by_year);

    console.log(JSON.stringify(before_stats, null, 4));

    await noInheritAllChildTables(inheritance_trees_by_year);

    await redefineRootTables(inheritance_trees_by_year);

    await redefineStateRootTables(inheritance_trees_by_year);

    await alterLeafTablesPrimaryKeys(inheritance_trees_by_year);

    const after_stats = await getSummaryStats(inheritance_trees_by_year);

    console.log(JSON.stringify({ before_stats, after_stats }, null, 4));

    if (!_.isEqual(before_stats, after_stats)) {
      throw new Error(`before_stats !== after_stats`);
    }
  }, PG_ENV);
}

main();
