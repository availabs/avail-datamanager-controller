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

async function moveStagedTranscomEventsToPublished(staging_schema: string) {
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

async function moveStagedEventsToAdminGeomsToPublished(staging_schema: string) {
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

async function moveStagedEventsToConflationMapToPublished(
  staging_schema: string
) {
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
    await moveStagedTranscomEventsToPublished(staging_schema);
    await moveStagedEventsToAdminGeomsToPublished(staging_schema);
    await moveStagedEventsToConflationMapToPublished(staging_schema);
  });

  final_event = { type: ":FINAL" };

  await dama_events.dispatch(final_event);
}
