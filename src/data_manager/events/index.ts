import dedent from "dedent";
import pgFormat from "pg-format";
import _ from "lodash";

import { FSA } from "flux-standard-action";

import DamaContextAttachedResource from "../contexts";

import dama_db from "../dama_db";

export type DamaEvent = FSA & {
  event_id: number;
  etl_context_id: number;
  _created_timestamp: Date;
};

class DamaEvents extends DamaContextAttachedResource {
  async spawnEtlContext(
    source_id: number | null = null,
    parent_context_id: number | null = null
  ): Promise<number> {
    const sql = `
      INSERT INTO data_manager.etl_contexts (
        parent_context_id,
        source_id
      ) VALUES ($1, $2)
      RETURNING etl_context_id
    `;

    const {
      // @ts-ignore
      rows: [{ etl_context_id: new_etl_context_id }],
    } = await dama_db.query({
      text: sql,
      values: [parent_context_id, source_id],
    });

    return new_etl_context_id;
  }

  async setEtlContextSourceId(etl_context_id: number, source_id: number) {
    const sql = `
      UPDATE data_manager.etl_contexts
        SET source_id = $1
        WHERE etl_context_id = $2
    `;

    await dama_db.query({
      text: sql,
      values: [source_id, etl_context_id],
    });
  }

  async dispatch(
    event: FSA,
    etl_context_id = this.etl_context_id
  ): Promise<DamaEvent> {
    const { type, meta = null, error = null } = event;

    // @ts-ignore
    let { payload = null }: { payload: FSA["payload"] | string | null } = event;

    //  Because JS Array types won't load into Postgres JSON columns, we must first stringify the array.
    //    See: https://github.com/brianc/node-postgres/issues/1519
    if (Array.isArray(payload)) {
      payload = JSON.stringify(payload);
    }

    const sql = dedent(`
      INSERT INTO data_manager.event_store (
        etl_context_id,
        type,
        payload,
        meta,
        error
      ) VALUES ( $1, $2, $3, $4, $5 )
        RETURNING *
    `);

    const values = [
      etl_context_id,
      type,
      payload || null,
      meta || null,
      error || false,
    ];

    const {
      rows: [dama_event],
    } = await dama_db.query({
      text: sql,
      values,
    });

    return dama_event;
  }

  // FIXME FIXME FIXME: RECURSIVE should be a configurable option.
  async queryEvents(
    since_event_id = -1,
    etl_context_id = this.etl_context_id
  ): Promise<DamaEvent[]> {
    const sql = dedent(`
      WITH RECURSIVE cte_ctx_tree(etl_context_id, parent_context_id) AS (
        SELECT
            etl_context_id,
            parent_context_id
          FROM data_manager.etl_contexts
          WHERE etl_context_id = $1
        UNION    
        SELECT
            a.etl_context_id,
            a.parent_context_id
          FROM data_manager.etl_contexts AS a
            INNER JOIN cte_ctx_tree
              ON (
                ( a.etl_context_id = cte_ctx_tree.parent_context_id )
                OR
                ( a.parent_context_id = cte_ctx_tree.etl_context_id )
              )
      )
        SELECT
            event_id,
            etl_context_id,
            type,
            payload,
            meta,
            error
          FROM data_manager.event_store AS a
            INNER JOIN cte_ctx_tree AS b
              USING (etl_context_id)
          WHERE ( event_id > $2 )
          ORDER BY event_id
    `);

    const { rows: dama_events } = await dama_db.query({
      text: sql,
      values: [etl_context_id, since_event_id],
    });

    // FIXME: Why are we doing this? Can it be deprecated without breaking anything?
    dama_events.forEach((e: FSA) => {
      // @ts-ignore
      e.meta = e.meta || {};
      // @ts-ignore
      e.meta.pgEnv = this.pg_env;
    });

    return dama_events;
  }

  async getInitialEvent(etl_context_id = this.etl_context_id) {
    const text = dedent(`
      SELECT
          b.*
        FROM data_manager.etl_contexts AS a
          INNER JOIN data_manager.event_store AS b
            ON ( a.initial_event_id = b.event_id )
        WHERE ( a.etl_context_id = $1 )
      ;
    `);

    const {
      rows: [initial_event],
    } = await dama_db.query({
      text,
      values: [etl_context_id],
    });

    if (!initial_event) {
      throw new Error(`No :INITIAL event for EtlContext ${etl_context_id}`);
    }

    return initial_event;
  }

  async getAllEtlContextEvents(etl_context_id = this.etl_context_id) {
    const text = dedent(`
      SELECT
          a.*
        FROM data_manager.event_store AS a
        WHERE ( etl_context_id = $1 )
        ORDER BY event_id
      ;
    `);

    const { rows: events } = await dama_db.query({
      text,
      values: [etl_context_id],
    });

    return events;
  }

  async queryOpenEtlProcessesStatusUpdatesForService(
    service_name: string
  ): Promise<DamaEvent[]> {
    const q = dedent(
      pgFormat(
        `
          SELECT
              * 
            FROM (
              SELECT
                  *,
                  row_number() OVER (PARTITION BY etl_context_id ORDER BY event_id DESC) AS row_number
                FROM data_manager.event_store
                  INNER JOIN (

                    SELECT
                        etl_context_id
                      FROM data_manager.event_store
                      WHERE ( type LIKE ( %L || '%' ) )
                    EXCEPT
                    SELECT
                        etl_context_id
                      FROM data_manager.event_store
                      WHERE (
                        ( type LIKE ( %L || '%' ) )
                        AND
                        (
                          ( right(type, 6) = ':FINAL' )
                          OR
                          ( right(type, 6) = ':ERROR' )
                        )
                      )

                   ) AS t USING (etl_context_id)
                WHERE ( right(type, 6) = 'UPDATE' )
            ) AS t
            WHERE ( row_number = 1 )
        `,
        service_name,
        service_name
      )
    );

    const { rows } = await dama_db.query(q);

    if (rows.length === 0) {
      return [];
    }

    return rows.map((row) => <DamaEvent>_.omit(row, "row_number"));
  }

  //  FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME
  //  These ASSUMPTIONS are no longer valid since Tasks support retry.
  //    TODO: Look at the repercussions of changing logic.
  //  ASSUMES the following CONVENTION/INVARIANTs:
  //    1. Event type prefixed by service_name. E.G:  `${service_name}/foo/bar:BAZ`
  //    2. All ETL processes for the service end with a ":FINAL" or ":ERROR" event
  //    3. All status update types match with /*UPDATE$/
  async queryEtlContextFinalEvent(
    etl_context_id = this.etl_context_id
  ): Promise<DamaEvent | null> {
    const q = dedent(
      `
        SELECT
            *
          FROM data_manager.event_store
          WHERE (
            ( etl_context_id = $1 )
            AND
            (
              ( right(type, 6) = ':FINAL' )
              OR
              ( right(type, 6) = ':ERROR' ) -- FIXME: Once RETRY is implemented, this no longer true.
            )
          )
      `
    );

    const { rows } = await dama_db.query({
      text: q,
      values: [etl_context_id],
    });

    if (rows.length === 0) {
      return null;
    }

    return rows[0];
  }
}

export default new DamaEvents();
