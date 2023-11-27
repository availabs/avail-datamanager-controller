//  ASSUMPTIONS TO TEST:
//    - Consistency of from/to_measures for dumped route Linestrings.
//      - No exclusive bound overlaps across observed measures
//    - No overlaps within a dumped MultiLineString's geometries.
//    - observed_1to1_lrs_mpt_to_aux_relations has every possible (table_name, ogc_fid)
//      except for the degenerate cases where geom is empty or measure is null.
//    - In the aux tables, from/to measures and geoms included in the MultiLinestring are consistent.
//    - The lrsn_milepoint and aux table (from_date, to_date) tuples are in sync.
//    - Optimism about milepoints prefectly overlapping aux shapes holds true in reality.
//  Reverse engineer the logic behind lrsn_milepoint from/to measures and direction.
//  Do the lrsn_milepoint and corresponding aux tables linestrings have the same directionality?
//
//  aux tables from/to_measures can be used to determine the from/to_measure units.
//  If spatial intersections reliable, use start/end points to partition

/*
dama_dev_1=# select public.st_length(public.st_transform(wkb_geometry, 26918)) * 0.00062136987761928244, from_measure, to_measure from ev_ris_collection_date where ogc_fid = 142;
 ?column?  | from_measure | to_measure
-----------+--------------+------------
 2.0674515 |            0 |  2.0674515
(1 row)

*/

const { join } = require('path')

const execa = require('execa')
const { Client } = require('pg')
const pgFormat = require('pg-format')
const _ = require('lodash')

const runEtlStep = require('./runEtlStep')

require('dotenv').config({ path: join(__dirname, '../../../../../config/postgres.dama_dev_1.env') })

const lrs_aux_tables = require('./lrs_aux_tables.json').sort()

const SOURCE_DATA_SCHEMA = 'nysdot_milepoint_2021'
const ETL_WORK_SCHEMA = 'nysdot_milepoint_2021_etl'

// const SOURCE_DATA_SCHEMA = 'nysdot_milepoint_subset'
// const ETL_WORK_SCHEMA = 'nysdot_milepoint_subset_etl'

process.env.SOURCE_DATA_SCHEMA = SOURCE_DATA_SCHEMA
process.env.ETL_WORK_SCHEMA = ETL_WORK_SCHEMA

const sql_dir = join(__dirname, './identify_source_data_integrity_issues_sql/')

const etl_step_configs = {
  inconsistent_measures: {
    rel_path: '01_lrs_aux_features_with_inconsistent_measures.sql',
  },
  possible_dupes: {
    rel_path: '02_possible_dupe_lrs_aux_features.sql',
  },
}

const workflow = [
  'inconsistent_measures',
  'possible_dupes'
]

async function main() {
  const db = new Client()

  try {
    await db.connect()

    for (const step of workflow) {
      if (typeof step === 'string') {
        const config = { sql_dir, ...etl_step_configs[step] }
        await runEtlStep(step, config)
      } else {
        await step()
      }
    }
  } catch (err) {
    console.error(err)
  } finally {
    await db.end()
  }
}

main()
