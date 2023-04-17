import dedent from "dedent";
import pgFormat from "pg-format";

import dama_db from "data_manager/dama_db";
import dama_events from "data_manager/events";
import logger from "data_manager/logger";
import { verifyIsInTaskEtlContext } from "data_manager/contexts";

import { conflation_version } from "../../constants/conflation_map_meta";

import getEtlContextLocalStateSqliteDb from "../../utils/getEtlContextLocalStateSqliteDb";

import { dbCols } from "./data_schema";

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
      `,
      staging_schema,
      staging_schema
    )
  );

  logger.silly(sql);

  await dama_db.query(sql);
}

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
      `,
      table_name, // DELETE FROM
      staging_schema, // USING
      table_name, // USING
      table_name, // INSERT
      staging_schema, // FROM
      table_name // from
    )
  );

  logger.silly(publish_sql);

  await dama_db.query(publish_sql);
}

async function createTranscomEventsOntoRoadNetworkView() {
  const events_onto_cmap_table_name = `transcom_events_onto_conflation_map_${conflation_version}`;

  const get_years_sql = dedent(
    pgFormat(
      `
        SELECT DISTINCT
            year
          FROM _transcom_admin.%I
      `,
      events_onto_cmap_table_name
    )
  );

  const { rows } = await dama_db.query(get_years_sql);

  const event_years = rows.map(({ year }) => year);

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

            FROM _transcom_admin.%I AS a
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
      events_onto_cmap_table_name,
      `conflation_map_${event_year}_${conflation_version}`,
      `conflation_map_${event_year}_nodes_${conflation_version}`,
      event_year
    );

    view_sql_select_stmts.push(sql);
  }

  const query_sql = view_sql_select_stmts.join(`
          UNION ALL`);

  const create_view_sql = dedent(
    pgFormat(
      `
        CREATE OR REPLACE VIEW _transcom_admin.%I
          AS ${query_sql}
      `,
      `transcom_events_onto_road_network_${conflation_version}`
    )
  );

  await dama_db.query(create_view_sql);
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
    await createTranscomEventsOntoRoadNetworkView();
  });

  final_event = { type: ":FINAL" };

  await dama_events.dispatch(final_event);
}
