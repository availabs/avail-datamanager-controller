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

import { PG_ENV, state_re } from "./domain";

import {
  InheritanceTreesByYear,
  parseLeafTableInfo,
  getTmcIdentificationInheritanceTreesByYear,
  getRootTable,
  getLeafTables,
} from "./utils/tmc_identification";

const tmc_identification_imports_schema =
  NpmrdsDatabaseSchemas.NpmrdsTmcIdentificationImports;

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
            download_timestamp       TIMESTAMP,

            PRIMARY KEY (tmc, state)
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
          table_schema
        )
      );

      await dama_db.query(sql);
    }
  }
}

async function moveLeafTablesToImportsDir(
  inheritance_trees_by_year: InheritanceTreesByYear
) {
  await dama_db.query(
    `CREATE SCHEMA IF NOT EXISTS ${tmc_identification_imports_schema}`
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
    values: [NpmrdsDataSources.NpmrdsTmcIdentificationImports],
  };

  const {
    rows: [{ source_id: tmc_identification_imports_source_id }],
  } = await dama_db.query(q);

  const view_exists_sql = dedent(`
    SELECT EXISTS (
      SELECT
          1
        FROM data_manager.views
        WHERE (
          ( table_schema = $1 )
          AND
          ( table_name = $2 )
        )
    ) AS view_exists
  `);

  const years = Object.keys(inheritance_trees_by_year).sort();

  for (const yr of years) {
    const inheritance_tree = inheritance_trees_by_year[yr];

    const leaf_tables = getLeafTables(inheritance_tree);

    for (const table_info of leaf_tables) {
      const { table_schema, table_name } = table_info;

      // NOTE: parseLeafTableInfo verifies table_name format.
      const { state, year, start_date, end_date, version } =
        parseLeafTableInfo(table_info);

      const state_root_table_name = `tmc_identification_${year}`;
      const new_table_name = `tmc_identification_${state}_${year}_${version}`;

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
          tmc_identification_imports_schema,
          tmc_identification_imports_schema,
          table_name,
          new_table_name
        )
      );

      await dama_db.query(move_leaf_table_sql);

      const must_alter_column_types_sql = dedent(
        pgFormat(
          `
              SELECT
                  column_name,
                  a.data_type
                FROM information_schema.columns AS a
                  INNER JOIN information_schema.columns AS b
                    USING (column_name)
                WHERE (
                  ( a.table_schema = %L )
                  AND
                  ( a.table_name = %L )
                  AND
                  ( b.table_schema = %L )
                  AND
                  ( b.table_name = %L )
                  AND
                  ( a.data_type <> b.data_type )
                )
              ;
            `,
          state,
          state_root_table_name,
          tmc_identification_imports_schema,
          new_table_name
        )
      );

      const { rows: must_alter_column_types } = await dama_db.query(
        must_alter_column_types_sql
      );

      for (const { column_name, data_type } of must_alter_column_types) {
        const alter_column_type_sql = dedent(
          pgFormat(
            `
              ALTER TABLE %I.%I
                ALTER COLUMN %I SET DATA TYPE %s
              ;
            `,
            tmc_identification_imports_schema,
            new_table_name,
            column_name,
            data_type
          )
        );

        await dama_db.query(alter_column_type_sql);
      }

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
          tmc_identification_imports_schema,
          new_table_name
        )
      );

      const { rows: check_constraints } = await dama_db.query(constraints_sql);

      const { conname: old_pkey_name } = check_constraints.find(
        ({ contype }) => contype === "p"
      )!;

      const new_pkey_name = `${new_table_name}_pkey`;

      if (new_pkey_name.length > 60) {
        throw new Error(`new_pkey_name.length = ${new_pkey_name.length}`);
      }

      const alter_primary_key_sql = dedent(
        pgFormat(
          `
            ALTER TABLE %I.%I
              DROP CONSTRAINT %I
            ;

            ALTER TABLE %I.%I
              ADD CONSTRAINT %I PRIMARY KEY (tmc, state)
            ;

            CLUSTER %I.%I USING %I ;
          `,
          tmc_identification_imports_schema,
          new_table_name,
          old_pkey_name,
          tmc_identification_imports_schema,
          new_table_name,
          new_pkey_name,
          tmc_identification_imports_schema,
          new_table_name,
          new_pkey_name
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
          state,
          state_root_table_name,
          tmc_identification_imports_schema,
          new_table_name,
          state
        )
      );

      await dama_db.query(attach_leaf_table_sql);

      for (const { conname, contype } of check_constraints) {
        // DROP CHECK CONSTRAINTs because they are redundant with PARTITIONed tables.
        if (contype === "c") {
          const drop_constraint_sql = dedent(
            pgFormat(
              `
                ALTER TABLE %I.%I
                  DROP CONSTRAINT %I
                ;
              `,
              tmc_identification_imports_schema,
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
        values: [table_schema, table_name],
      });

      if (view_exists) {
        const table_full_name = pgFormat("%I.%I", table_schema, table_name);

        throw new Error(
          `dama_manager.view already exists for ${table_full_name}`
        );
      }

      const geography_version = stateAbbr2FipsCode[state];

      const version_timestamp = version.slice(1);

      const download_timestamp = DateTime.fromFormat(
        version_timestamp,
        "yyyyMMdd't'HHmmss"
      ).toISO();

      const view_meta = {
        source_id: tmc_identification_imports_source_id,
        interval_version: `${year}`,
        geography_version,
        version: table_name,
        table_schema: tmc_identification_imports_schema,
        table_name: new_table_name,
        start_date,
        end_date,
        last_updated: version_timestamp,
        metadata: {
          year,
          state,
          table_schema: tmc_identification_imports_schema,
          table_name: new_table_name,
          download_timestamp,
        },
      };

      await dama_meta.createNewDamaView(view_meta);
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

    await moveLeafTablesToImportsDir(inheritance_trees_by_year);

    const after_stats = await getSummaryStats(inheritance_trees_by_year);

    console.log(JSON.stringify({ before_stats, after_stats }, null, 4));

    if (!_.isEqual(before_stats, after_stats)) {
      throw new Error(`before_stats !== after_stats`);
    }
  }, PG_ENV);
}

main();
