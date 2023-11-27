

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

const SOURCE_DATA_SCHEMA = 'nysdot_milepoint_2020'
const ETL_WORK_SCHEMA = 'nysdot_milepoint_2020_etl'

// const SOURCE_DATA_SCHEMA = 'nysdot_milepoint_subset'
// const ETL_WORK_SCHEMA = 'nysdot_milepoint_subset_etl'

process.env.SOURCE_DATA_SCHEMA = SOURCE_DATA_SCHEMA
process.env.ETL_WORK_SCHEMA = ETL_WORK_SCHEMA

const sql_dir = join(__dirname, './assign_lrs_milepoint_measures_sql/')

const etl_step_configs = {
  clean: {
    rel_path: '00_create_clean_etl_schema.sql',
  },
  createLRSMilepointLinestringsTable: {
    rel_path: '01_create_lrs_milepoint_linestrings.sql',
  },
  createLRSAuxLinestringsTable: {
    rel_path: '02_create_lrs_aux_linestrings.sql',
  },
  doNonpartitionedMptLinestringsFromToAssignments: {
    rel_paths: [
      '03_initialize_nonpartitioned_mpt_linestrings_from_to_assignments.sql',
      '04_do_nonpartitioned_mpt_linestrings_from_to_assignments.sql'
    ]
  },
  qa: {
    rel_paths: [
      'qa_lrs_aux_linestrings_table.sql',
      'qa_lrs_mpt_linestrings_from_to_mi_assignments.sql'
    ]
  },
}

const workflow = [
  'clean',
  'createLRSMilepointLinestringsTable',
  createLrsAuxGeometriesTable,
  'createLRSAuxLinestringsTable',
  'doNonpartitionedMptLinestringsFromToAssignments',
  'qa',
]

async function createLrsAuxGeometriesTable() {
  console.log('createLrsAuxGeometriesTable')
  const db = new Client()

  const union_sub_queries_arr = []

  try {
    await db.connect()

    await db.query('BEGIN ;')

    const create_tmp_lrs_milepoint_route_ids_sql = pgFormat(`
      CREATE TEMPORARY TABLE tmp_lrs_milepoint_route_ids (
        route_id    TEXT PRIMARY KEY
      ) WITH (fillfactor=100) ;

      INSERT INTO tmp_lrs_milepoint_route_ids (
        route_id
      )
        SELECT DISTINCT
            route_id
          FROM %I.lrs_milepoint_linestrings
      ;

      CLUSTER tmp_lrs_milepoint_route_ids
        USING tmp_lrs_milepoint_route_ids_pkey
      ;
    `, ETL_WORK_SCHEMA // FROM
    )

    await db.query(create_tmp_lrs_milepoint_route_ids_sql)

    const create_table_sql = pgFormat(`
      DROP TABLE IF EXISTS %I.lrs_aux_geometries CASCADE ;

      CREATE TABLE %I.lrs_aux_geometries (
        lrs_aux_table_name                          TEXT NOT NULL,
        lrs_aux_ogc_fid                             INTEGER NOT NULL,

        route_id                                    TEXT,

        lrs_aux_from_date                           TIMESTAMP WITH TIME ZONE,

        lrs_aux_geom_from_measure                   DOUBLE PRECISION NOT NULL,
        lrs_aux_geom_to_measure                     DOUBLE PRECISION NOT NULL,

        lrs_aux_geom_shape_length                   DOUBLE PRECISION NOT NULL,
        lrs_aux_geom_len_mi                         DOUBLE PRECISION NOT NULL,
        has_reliable_geom_from_to_measures          BOOLEAN NOT NULL,

        wkb_geometry                                public.geometry(MultiLineString,4326),

        PRIMARY KEY (lrs_aux_table_name, lrs_aux_ogc_fid)
      )
    `, ETL_WORK_SCHEMA, ETL_WORK_SCHEMA)

    await db.query(create_table_sql)

    for (const lrs_aux_table_name of lrs_aux_tables) {
      console.log(lrs_aux_table_name)
      const has_wkb_geometry_column_sql = `
        SELECT NOT EXISTS (
          SELECT
              1
            FROM information_schema.columns
            WHERE (
              ( table_schema = $1 )
              AND
              ( table_name = $2 )
              AND
              ( column_name = 'wkb_geometry' )
            )
        ) AS does_not_have_wkb_geometry_column
      `

      const {
        rows: [{ does_not_have_wkb_geometry_column }]
      } = await db.query(has_wkb_geometry_column_sql, [SOURCE_DATA_SCHEMA, lrs_aux_table_name]) ;

      if (does_not_have_wkb_geometry_column) {
        console.log('Skipping', lrs_aux_table_name, 'because does not have a wkb_geometry column.')
        continue
      }

      const insert_sql = pgFormat(`
        INSERT INTO %I.lrs_aux_geometries (
          lrs_aux_table_name,
          lrs_aux_ogc_fid,

          route_id,

          lrs_aux_from_date,

          lrs_aux_geom_from_measure,
          lrs_aux_geom_to_measure,

          lrs_aux_geom_shape_length,
          lrs_aux_geom_len_mi,
          has_reliable_geom_from_to_measures,

          wkb_geometry
        )
          SELECT
              lrs_aux_table_name,
              lrs_aux_ogc_fid,

              route_id,

              lrs_aux_from_date,

              lrs_aux_geom_from_measure,
              lrs_aux_geom_to_measure,

              lrs_aux_geom_shape_length,
              lrs_aux_geom_len_mi,

              (
                (
                  ABS(
                    ABS(lrs_aux_geom_from_measure - lrs_aux_geom_to_measure)
                    - lrs_aux_geom_len_mi
                  ) * 5280
                ) <= 10 /*feet*/
              ) AS has_reliable_geom_from_to_measures,

              wkb_geometry

            FROM (
              SELECT
                  $1 AS lrs_aux_table_name,
                  ogc_fid AS lrs_aux_ogc_fid,

                  route_id,

                  ${lrs_aux_table_name !== 'redline' ? 'from_date' : 'effective_date'} AS lrs_aux_from_date,

                  from_measure AS lrs_aux_geom_from_measure,
                  to_measure AS lrs_aux_geom_to_measure,

                  shape_length AS lrs_aux_geom_shape_length,

                  (
                    ST_Length(
                      ST_Transform(
                        wkb_geometry,
                        26918
                      )
                    ) * 0.00062136987761928244
                  ) AS lrs_aux_geom_len_mi,

                  wkb_geometry

                FROM %I.%I
                  INNER JOIN tmp_lrs_milepoint_route_ids
                    USING (route_id)

                WHERE (
                  -- NOTE: only redline table does not have the to_date column.
                  ${ lrs_aux_table_name !== 'redline' ? '( to_date IS NULL ) AND' : ''}
                  ( from_measure IS NOT NULL )
                  AND
                  ( to_measure IS NOT NULL )
                  AND
                  ( wkb_geometry IS NOT NULL )
                )

            ) AS t
        `,
        // INSERT schema
        ETL_WORK_SCHEMA, // INSERT
        // FROM table_schema, table_naem
        SOURCE_DATA_SCHEMA,
        lrs_aux_table_name
      )

      await db.query(insert_sql, [lrs_aux_table_name])
    }

    await db.query('COMMIT ;')

  } finally {
    await db.end()
  }
}

async function createLrsMidpointLineStringIntersectionsAnalysis(db) {
  console.time('createLrsMidpointLineStringIntersectionsAnalysis')
  await db.query('BEGIN ;')

  const create_sql = pgFormat(`
    DROP TABLE IF EXISTS %I.lts_mpt_linestring_intersections_analysis CASCADE ;

    CREATE TABLE %I.lts_mpt_linestring_intersections_analysis (
      lrs_mpt_lstr_id                   INTEGER PRIMARY KEY,
      lrs_mpt_ogc_fid                   INTEGER NOT NULL,
      lrs_mpt_lstr_idx                  INTEGER NOT NULL,

      lrs_aux_lstr_meta                 JSONB NOT NULL,
      lrs_aux_lstr_count                INTEGER NOT NULL,

      lrs_aux_lstr_covers_mpt_lstr      INTEGER[] NOT NULL,
      lrs_aux_lstr_from_gte_to_measure  INTEGER[] NOT NULL,

      min_lrs_aux_from_measure          DOUBLE PRECISION NOT NULL,
      max_lrs_aux_to_measure            DOUBLE PRECISION NOT NULL,

      all_from_measure_lt_to_measure    BOOLEAN NOT NULL,
      intersection_linestrings_count    INTEGER NOT NULL,

      intersections_cover_lrs_mpt_lstr  BOOLEAN NOT NULL
    ) WITH ( fillfactor=100 ) ;
  `, ETL_WORK_SCHEMA, ETL_WORK_SCHEMA );

  await db.query(create_sql)

  const load_sql = pgFormat(`
    INSERT INTO %I.lts_mpt_linestring_intersections_analysis (
      lrs_mpt_lstr_id,
      lrs_mpt_ogc_fid,
      lrs_mpt_lstr_idx,

      lrs_aux_lstr_meta,
      lrs_aux_lstr_count,
      lrs_aux_lstr_covers_mpt_lstr,
      lrs_aux_lstr_from_gte_to_measure,
      min_lrs_aux_from_measure,
      max_lrs_aux_to_measure,
      all_from_measure_lt_to_measure,
      intersection_linestrings_count,
      intersections_cover_lrs_mpt_lstr
    )
      SELECT
          lrs_mpt_lstr_id,

          a.lrs_mpt_ogc_fid,
          a.lrs_mpt_lstr_idx,

          jsonb_agg(
            json_build_object(
              'ogc_fid',          a.lrs_aux_ogc_fid,
              'geom_idx',         a.lrs_aux_lstr_idx
            ) ORDER BY a.lrs_aux_ogc_fid, a.lrs_aux_lstr_idx
          ) AS lrs_aux_lstr_meta,

          COUNT(1) AS lrs_aux_lstr_count,

          COALESCE(
            ARRAY_AGG(a.lrs_aux_lstr_id ORDER BY a.lrs_aux_lstr_id)
              FILTER ( WHERE
                ST_Covers(
                  ST_CollectionExtract(a.wkb_geometry, 2),
                  b.wkb_geometry
                )
              ),
              (ARRAY[])::INTEGER[]
          ) AS lrs_aux_lstr_covers_mpt_lstr,

          COALESCE(
            ARRAY_AGG(a.lrs_aux_lstr_id ORDER BY a.lrs_aux_lstr_id)
              FILTER ( WHERE (a.lrs_aux_from_measure >= a.lrs_aux_to_measure) ),
            (ARRAY[])::INTEGER[]
          ) AS lrs_aux_lstr_from_gte_to_measure,

          MIN(a.lrs_aux_from_measure) AS min_lrs_aux_from_measure,
          MAX(a.lrs_aux_to_measure) AS max_lrs_aux_to_measure,
          BOOL_AND(a.lrs_aux_from_measure < a.lrs_aux_to_measure),

          SUM(
            ST_NumGeometries(
              ST_Multi(
                ST_CollectionExtract(
                  a.wkb_geometry,
                  2
                )
              )
            )
          ) AS intersection_linestrings_count,

          ST_Covers(
            ST_Union(
              ST_CollectionExtract(
                a.wkb_geometry,
                2
              )
            ),
            ST_Union(b.wkb_geometry)
          ) AS intersections_cover_lrs_mpt_lstr

        FROM %I.lrs_mpt_to_aux_linestring_intersections AS a
          INNER JOIN %I.lrs_milepoint_linestrings AS b
            USING (lrs_mpt_lstr_id)

        GROUP BY 1, 2, 3
  `, ETL_WORK_SCHEMA, ETL_WORK_SCHEMA, ETL_WORK_SCHEMA)

  await db.query(load_sql)

  await db.query('COMMIT ;')
  console.timeEnd('createLrsMidpointLineStringIntersectionsAnalysis')
}

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

