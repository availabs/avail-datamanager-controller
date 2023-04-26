/*
 TODO: Move all CLUSTER and ANALYZE to a separate task called optimize.
       Have a config flag --optimize.
       This would allow us to only do the locking CLUSTERs during the nightly ETL process.
       We could then update the TRANSCOM events during the day with little client-side interruption.
*/
import dedent from "dedent";
import pgFormat from "pg-format";

import dama_db from "data_manager/dama_db";
import dama_events from "data_manager/events";
import logger from "data_manager/logger";
import { verifyIsInTaskEtlContext } from "data_manager/contexts";

import { dbCols } from "../../domain";

import {
  conflation_version,
  min_year as min_conflation_map_year,
  max_year as max_confltion_map_year,
} from "../../constants/conflation_map_meta";

import getEtlContextLocalStateSqliteDb from "../../utils/getEtlContextLocalStateSqliteDb";

export type InitialEvent = {
  type: ":INITIAL";
  payload: {
    etl_work_dir: string;
  };
};

export type FinalEvent = {
  type: ":FINAL";
};

async function publishTranscomEvents(staging_schema: string) {
  const indent = "".repeat(10);

  const conflictActionsHolders = dbCols
    .map(() => `${indent}%I = EXCLUDED.%I`)
    .join(",\n");

  const conflictActionFillers = dbCols.reduce((acc: string[], col) => {
    acc.push(col);
    acc.push(col);
    return acc;
  }, []);

  const sql = dedent(
    pgFormat(
      `
        -- NOTE: If we use DELETE before INSERT, we lose the _created_timestamp.

        INSERT INTO _transcom_admin.transcom_events_expanded
          SELECT
              *
            FROM %I.transcom_events_expanded
            ON CONFLICT ON CONSTRAINT transcom_events_expanded_pkey
              DO UPDATE
                SET -- SEE: https://stackoverflow.com/a/40689501/3970755
                  ${conflictActionsHolders}
        ;

        CLUSTER _transcom_admin.transcom_events_expanded ;

        ANALYZE _transcom_admin.transcom_events_expanded ;
      `,
      staging_schema,
      ...conflictActionFillers
    )
  );

  logger.silly(sql);

  await dama_db.query(sql);
}

async function publishTranscomEventsToAdminGeoms(staging_schema: string) {
  const sql = dedent(
    pgFormat(
      `
        DELETE FROM _transcom_admin.transcom_event_administative_geographies
          WHERE event_id IN (
            SELECT DISTINCT
                event_id
              FROM %I.transcom_event_administative_geographies
          )
        ;

        INSERT INTO _transcom_admin.transcom_event_administative_geographies (
          event_id,
          state_name,
          state_code,
          region_name,
          region_code,
          county_name,
          county_code,
          mpo_name,
          mpo_code,
          ua_name,
          ua_code
        )
          SELECT
              event_id,
              state_name,
              state_code,
              region_name,
              region_code,
              county_name,
              county_code,
              mpo_name,
              mpo_code,
              ua_name,
              ua_code
            FROM %I.transcom_event_administative_geographies
        ;

        CLUSTER _transcom_admin.transcom_event_administative_geographies ;

        ANALYZE _transcom_admin.transcom_event_administative_geographies ;
      `,
      staging_schema,
      staging_schema
    )
  );

  logger.silly(sql);

  await dama_db.query(sql);
}

// FIXME: This currently does not support changing the authoritative conflation_version.
async function publishTranscomEventsToConflationMap(staging_schema: string) {
  const table_name = `transcom_events_onto_conflation_map_${conflation_version}`;

  const {
    rows: [{ must_create_table }],
  } = await dama_db.query(
    dedent(
      pgFormat(
        `
          SELECT NOT EXISTS (
            SELECT
                1
              FROM pg_catalog.pg_tables
              WHERE (
                ( schemaname = '_transcom_admin' )
                AND
                ( tablename = %L )
              )
          ) AS must_create_table ;
        `,
        table_name
      )
    )
  );

  if (must_create_table) {
    const sql = dedent(
      pgFormat(
        `
          CREATE TABLE _transcom_admin.%I (
            LIKE transcom.transcom_events_onto_conflation_map
            INCLUDING ALL
          ) ;

          CREATE TRIGGER update_modified_timestamp_trigger
            BEFORE
              UPDATE ON _transcom_admin.%I
            FOR EACH ROW
              EXECUTE PROCEDURE _transcom_admin.update_modified_timestamp_trigger_fn()
          ;

          CLUSTER _transcom_admin.%I
            USING %I
          ;
        `,
        table_name,
        table_name,
        table_name,
        `${table_name}_pkey`
      )
    );

    await dama_db.query(sql);

    // Does the root table already have a child table?
    // Note:  This could be a previous conflation_version.
    //        FIXME: We need to support updating the authoritative conflation_version via data_manager.
    const {
      rows: [{ must_inherit_table }],
    } = await dama_db.query(
      dedent(
        pgFormat(
          `
          SELECT NOT EXISTS (
            SELECT
                1
              FROM pg_catalog.pg_inherits
              WHERE (
                inhparent = 'transcom.transcom_events_onto_conflation_map'::regclass::oid
              )
          ) AS must_inherit_table ;
        `
        )
      )
    );

    if (must_inherit_table) {
      // If not, new table inherits
      await dama_db.query(
        dedent(
          pgFormat(
            `
              ALTER TABLE _transcom_admin.%I
                INHERIT transcom.transcom_events_onto_conflation_map
            `,
            table_name
          )
        )
      );
    }
  }

  const publish_sql = dedent(
    pgFormat(
      `
        DELETE FROM _transcom_admin.%I AS a
          USING %I.%I AS b
          WHERE (
            ( a.event_id = b.event_id )
            AND
            ( a.year = b.year )
          )
        ;

        INSERT INTO _transcom_admin.%I (
          event_id,
          year,
          conflation_way_id,
          conflation_node_id,
          osm_fwd,
          both_directions,
          n,

          snap_pt_geom
        )
          SELECT
              event_id,
              year,
              conflation_way_id,
              conflation_node_id,
              osm_fwd,
              both_directions,
              n,

              snap_pt_geom
            FROM %I.%I
        ;

        CLUSTER _transcom_admin.%I ;

        ANALYZE _transcom_admin.%I ;
      `,
      table_name, // DELETE FROM
      staging_schema, // USING
      table_name, // USING
      table_name, // INSERT
      staging_schema, // FROM
      table_name, // FROM
      table_name, // CLUSTER
      table_name // ANALYZE
    )
  );

  logger.silly(publish_sql);

  await dama_db.query(publish_sql);
}

async function updateTranscomEventsOntoRoadNetwork() {
  const must_create_matview_sql = dedent(
    pgFormat(
      `
        -- Check if the MATERIALIZED VIEW does not exist
        SELECT NOT EXISTS (
          SELECT
              1
            FROM pg_catalog.pg_matviews
            WHERE (
              ( schemaname = 'transcom' )
              AND
              ( matviewname = 'transcom_events_onto_road_network' )
            )
        ) AS must_create_matview
      `
    )
  );

  let {
    rows: [{ must_create_matview }],
  } = await dama_db.query(must_create_matview_sql);

  if (!must_create_matview) {
    const has_new_years_sql = dedent(
      pgFormat(
        `
          -- Check if the MATERIALIZED VIEW needs to be redefined because there are new event years.
          SELECT EXISTS (
            SELECT
                year
              FROM transcom.transcom_events_onto_conflation_map
            EXCEPT
            SELECT
                year
              FROM transcom.transcom_events_onto_road_network
          ) AS has_new_years
        `
      )
    );

    const {
      rows: [{ has_new_years }],
    } = await dama_db.query(has_new_years_sql);

    must_create_matview = has_new_years;
  }

  if (must_create_matview) {
    const get_years_sql = dedent(
      `
        SELECT DISTINCT
            year
          FROM transcom.transcom_events_aggregate
          ORDER BY year
      `
    );

    const { rows } = await dama_db.query(get_years_sql);

    const event_years = rows
      .map(({ year }) => +year)
      .filter(
        (year) =>
          year >= min_conflation_map_year && year <= max_confltion_map_year
      );

    const view_sql_select_stmts: string[] = [];

    for (let event_year of event_years) {
      event_year = +event_year;

      const sql = pgFormat(
        `
          SELECT
              a.event_id,
              a.year,

              b.event_type,
              e.general_category    AS nysdot_general_category,
              e.sub_category        AS nysdot_sub_category,
              e.detailed_category   AS nysdot_detailed_category,

              GREATEST(
                b.start_date_time,
                %L::TIMESTAMP                           -- <event_year>-01-01
              ) AS start_date_time,

              LEAST(
                b.close_date,
                %L::TIMESTAMP - '1 second'::INTERVAL    -- <event_year + 1>-01-01
              ) AS close_date,

              a.conflation_way_id,
              a.conflation_node_id,

              CASE
                WHEN a.osm_fwd = 0
                  THEN -a.conflation_node_id
                ELSE a.conflation_node_id
              END AS signed_conflation_node_id,

              c.dir,
              a.n,
              c.osm,
              a.osm_fwd,
              a.both_directions,
              c.ris,
              c.tmc,

              a._modified_timestamp AS transcom_event_modified_timestamp,

              b.point_geom    AS transcom_event_point_geom,
              a.snap_pt_geom  AS transcom_event_snapped_geom,
              c.wkb_geometry  AS conflation_map_way_geom,
              d.wkb_geometry  AS conflation_map_node_geom

            FROM transcom.transcom_events_onto_conflation_map AS a
              INNER JOIN _transcom_admin.transcom_events_expanded_view AS b
                USING (event_id)
              INNER JOIN conflation.%I AS c
                ON ( a.conflation_way_id = c.id )
              INNER JOIN conflation.%I AS d
                ON ( a.conflation_node_id = d.id )
              LEFT OUTER JOIN transcom.nysdot_transcom_event_classifications AS e
                ON ( LOWER(b.event_type) = LOWER(e.event_type) )

            WHERE ( a.year = %s )
        `,
        `${event_year}-01-01`,
        `${event_year + 1}-01-01`,
        `conflation_map_${event_year}_${conflation_version}`,
        `conflation_map_${event_year}_nodes_${conflation_version}`,
        event_year
      );

      view_sql_select_stmts.push(sql);
    }

    const query_sql = view_sql_select_stmts.join(`
          UNION ALL`);

    const create_view_sql = dedent(
      `
        DROP MATERIALIZED VIEW IF EXISTS transcom.transcom_events_onto_road_network CASCADE ;

        CREATE MATERIALIZED VIEW transcom.transcom_events_onto_road_network
          AS ${query_sql}
        ;

        CREATE INDEX transcom_events_onto_road_network_pkey
          ON transcom.transcom_events_onto_road_network (event_id, year)
          WITH (fillfactor=100)
        ;

        CREATE INDEX transcom_events_onto_road_network_tmc_idx
          ON transcom.transcom_events_onto_road_network (tmc, year)
          WITH (fillfactor=100)
        ;

        CLUSTER transcom.transcom_events_onto_road_network
          USING transcom_events_onto_road_network_pkey
        ;

        ANALYZE transcom.transcom_events_onto_road_network ;
      `
    );

    await dama_db.query(create_view_sql);
  } else {
    const refresh_matview_sql = dedent(
      `
        REFRESH MATERIALIZED VIEW transcom.transcom_events_onto_road_network ;

        CLUSTER transcom.transcom_events_onto_road_network ;

        ANALYZE transcom.transcom_events_onto_road_network ;
      `
    );

    await dama_db.query(refresh_matview_sql);
  }
}

async function updateTranscomEventsByTmcSummary() {
  const must_create_matview_sql = dedent(
    pgFormat(
      `
        SELECT NOT EXISTS (
          SELECT
              1
            FROM pg_catalog.pg_matviews
            WHERE (
              ( schemaname = 'transcom' )
              AND
              ( matviewname = 'transcom_events_by_tmc_summary' )
            )
        ) AS must_create_matview
      `
    )
  );

  const {
    rows: [{ must_create_matview }],
  } = await dama_db.query(must_create_matview_sql);

  if (must_create_matview) {
    const create_matview_sql = dedent(
      `
        DROP MATERIALIZED VIEW IF EXISTS transcom.transcom_events_by_tmc_summary CASCADE;

        CREATE MATERIALIZED VIEW transcom.transcom_events_by_tmc_summary
          AS
            SELECT
                tmc,
                year,
                COALESCE(t1.accident_counts_by_type, '{}'::JSONB) AS accident_counts_by_type,
                COALESCE(t1.total_accidents, 0)::INTEGER AS total_accidents,
                COALESCE(t3.total_construction_days, 0)::INTEGER AS total_construction_days
              FROM (
                SELECT DISTINCT
                    tmc,
                    year
                  FROM transcom.transcom_events_onto_road_network
              ) AS t0
                LEFT OUTER JOIN (
                  SELECT
                      tmc,
                      year,
                      jsonb_object_agg(
                        x.nysdot_detailed_category,
                        x.event_type_ct
                      ) AS accident_counts_by_type,

                      SUM(x.event_type_ct) AS total_accidents

                    FROM (
                      SELECT
                          tmc,
                          year,
                          nysdot_detailed_category,
                          count(1) AS event_type_ct
                      FROM transcom.transcom_events_onto_road_network AS a
                      WHERE ( nysdot_sub_category = 'Crash' )
                      GROUP BY tmc, year, nysdot_detailed_category
                    ) AS x

                    GROUP BY tmc, year
                ) AS t1 USING (tmc, year)
                LEFT OUTER JOIN (
                  SELECT
                      tmc,
                      year,
                      COUNT(DISTINCT event_date) AS total_construction_days
                    FROM (
                      SELECT
                          tmc,
                          year,
                          generate_series(
                            date_trunc('day', close_date),
                            date_trunc('day', start_date_time),
                            '1 day'::interval
                          ) AS event_date
                        FROM transcom.transcom_events_onto_road_network AS a
                        WHERE ( nysdot_sub_category = 'Construction' )
                    ) AS x
                    GROUP BY tmc, year
                ) AS t3 USING (tmc, year)
        ;

        CREATE INDEX transcom_events_by_tmc_summary_pkey
          ON transcom.transcom_events_by_tmc_summary (tmc, year)
          WITH (fillfactor=100)
        ;

        CLUSTER transcom.transcom_events_by_tmc_summary
          USING transcom_events_by_tmc_summary_pkey ;

        ANALYZE transcom.transcom_events_by_tmc_summary ;
      `
    );

    await dama_db.query(create_matview_sql);
  } else {
    const refresh_matview_sql = dedent(
      `
        REFRESH MATERIALIZED VIEW transcom.transcom_events_by_tmc_summary ;

        CLUSTER transcom.transcom_events_by_tmc_summary ;

        ANALYZE transcom.transcom_events_by_tmc_summary ;
      `
    );

    await dama_db.query(refresh_matview_sql);
  }
}

// NOTE:  Should not be run as a Queued Subtask because will need to be
//        in the same TRANSACTION as post-processing tasks.
export default async function main(etl_work_dir: string) {
  verifyIsInTaskEtlContext();

  const events = await dama_events.getAllEtlContextEvents();

  let final_event = events.find(({ type }) => type === ":FINAL");

  if (final_event) {
    return final_event;
  }

  const sqlite_db = getEtlContextLocalStateSqliteDb(etl_work_dir);

  const staging_schema = sqlite_db
    .prepare(
      `
        SELECT
            staging_schema
          FROM etl_context
      `
    )
    .pluck()
    .get();

  await dama_db.runInTransactionContext(async () => {
    await publishTranscomEvents(staging_schema);
    await publishTranscomEventsToAdminGeoms(staging_schema);
    await publishTranscomEventsToConflationMap(staging_schema);
    await updateTranscomEventsOntoRoadNetwork();
    await updateTranscomEventsByTmcSummary();
  });

  final_event = { type: ":FINAL" };

  await dama_events.dispatch(final_event);
}
