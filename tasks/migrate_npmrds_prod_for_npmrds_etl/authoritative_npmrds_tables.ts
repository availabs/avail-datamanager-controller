import _ from "lodash";
import dedent from "dedent";
import pgFormat from "pg-format";

import dama_db from "../../src/data_manager/dama_db";

import { NpmrdsState } from "../../src/data_types/npmrds/domain";

import makeTravelTimesExportTablesAuthoritative from "../../src/data_types/npmrds/dt-npmrds_travel_times/actions/makeTravelTimesExportTablesAuthoritative";

import { PG_ENV, InheritanceTree } from "./domain";

import {
  getNpmrdsTablesInheritanceTree,
  getLeafTables,
  parseLeafTableInfo,
} from "./utils/npmrds_travel_times.migrated";

const state_import_date_ranges_without_gaps: Record<
  NpmrdsState,
  {
    start_date: string;
    end_date: string;
  }
> = {
  [NpmrdsState.ct]: { start_date: "2021-01-01", end_date: "2022-12-31" },
  [NpmrdsState.nj]: { start_date: "2016-01-01", end_date: "2023-04-30" },
  [NpmrdsState.ny]: { start_date: "2016-01-01", end_date: "2022-05-31" }, // BUG. Should have been 2023-04-30
  [NpmrdsState.pa]: { start_date: "2021-01-01", end_date: "2022-12-31" },

  [NpmrdsState.on]: { start_date: "2021-01-01", end_date: "2022-05-31" },
  [NpmrdsState.qc]: { start_date: "2021-01-01", end_date: "2022-05-31" },
};

async function detachAllLeafTables(inheritance_tree: InheritanceTree) {
  const leaf_tables = getLeafTables(inheritance_tree);

  for (const {
    parent_table_schema,
    parent_table_name,
    table_schema,
    table_name,
  } of leaf_tables) {
    if (parent_table_name) {
      const sql = dedent(
        pgFormat(
          `
            ALTER TABLE %I.%I
              DETACH PARTITION %I.%I
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

function getLeafTablesInNoGapRanges(inheritance_tree: InheritanceTree) {
  const leaves = getLeafTables(inheritance_tree);

  const no_gap_leaves = leaves.filter((table_info) => {
    const { state, start_date, end_date } = parseLeafTableInfo(table_info);

    const range = state_import_date_ranges_without_gaps[state];

    return start_date >= range.start_date && end_date <= range.end_date;
  });

  console.log(JSON.stringify({ no_gap_leaves }, null, 4));

  return no_gap_leaves;
}

async function getAllDamaViewIdsToMakeAuthoritative(
  inheritance_tree: InheritanceTree
) {
  await dama_db.query(
    `
      CREATE TEMPORARY TABLE tmp_leaf_tables (
        table_schema  TEXT,
        table_name    TEXT,

        PRIMARY KEY (table_schema, table_name)
      ) ON COMMIT DROP ;
    `
  );

  const insert_leaf_table_sql = dedent(
    `
      INSERT INTO tmp_leaf_tables (table_schema, table_name)
        VALUES ( $1, $2 )
      ;
    `
  );

  const nongap_leaf_tables = getLeafTablesInNoGapRanges(inheritance_tree);

  for (const { table_schema, table_name } of nongap_leaf_tables) {
    await dama_db.query({
      text: insert_leaf_table_sql,
      values: [table_schema, table_name],
    });
  }

  const { rows } = await dama_db.query(
    `
      SELECT
          view_id
        FROM data_manager.views
          INNER JOIN tmp_leaf_tables
            USING (table_schema, table_name)
        ORDER BY view_id
    `
  );

  return rows.map(({ view_id }) => view_id);
}

async function verifyNewInheritanceTree(
  before_inheritance_tree: InheritanceTree,
  after_inheritance_tree: InheritanceTree
) {
  const before_leaves = getLeafTables(before_inheritance_tree);
  const before_nonleaves = _.differenceWith(
    before_inheritance_tree,
    before_leaves,
    _.isEqual
  );

  const before_leaves_to_attach = getLeafTablesInNoGapRanges(
    before_inheritance_tree
  );

  const after_leaves = getLeafTables(after_inheritance_tree);

  const after_nonleaves = _.differenceWith(
    after_inheritance_tree,
    after_leaves,
    _.isEqual
  );

  const after_leaves_to_attach = getLeafTablesInNoGapRanges(
    after_inheritance_tree
  );

  if (!_.isEqual(before_nonleaves, after_nonleaves)) {
    console.error(
      JSON.stringify({ before_nonleaves, after_nonleaves }, null, 4)
    );
    throw new Error(
      "INVARIANT VIOLATION: before_nonleaves !== after_nonleaves"
    );
  }

  if (!_.isEqual(before_leaves_to_attach, after_leaves)) {
    console.error(
      JSON.stringify({ before_leaves_to_attach, after_leaves }, null, 4)
    );
    throw new Error(
      "INVARIANT VIOLATION: before_leaves_to_attach !== after_leaves"
    );
  }

  if (!_.isEqual(after_leaves, after_leaves_to_attach)) {
    console.error(
      JSON.stringify({ after_leaves, after_leaves_to_attach }, null, 4)
    );
    throw new Error(
      "INVARIANT VIOLATION: after_leaves !== after_leaves_to_attach"
    );
  }
}

async function main() {
  dama_db.runInTransactionContext(async () => {
    const before_inheritance_tree = await getNpmrdsTablesInheritanceTree();

    await detachAllLeafTables(before_inheritance_tree);

    const authoritative_view_ids = await getAllDamaViewIdsToMakeAuthoritative(
      before_inheritance_tree
    );

    await makeTravelTimesExportTablesAuthoritative(authoritative_view_ids);

    const after_inheritance_tree = await getNpmrdsTablesInheritanceTree();

    console.log(
      JSON.stringify(
        { before_inheritance_tree, after_inheritance_tree },
        null,
        4
      )
    );

    verifyNewInheritanceTree(before_inheritance_tree, after_inheritance_tree);
  }, PG_ENV);
}

main();
