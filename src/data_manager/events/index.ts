import dedent from "dedent";
import pgFormat from "pg-format";
import _ from "lodash";

import createPostgresSubscriber, {
  Subscriber as PgListenSubscriber,
} from "pg-listen";

import { FSA } from "flux-standard-action";

import DamaContextAttachedResource from "../contexts";

import dama_db from "../dama_db";

import {
  PgEnv,
  getPostgresConnectionUri,
} from "../dama_db/postgres/PostgreSQL";

export type DamaEvent = FSA & {
  event_id: number;
  etl_context_id: number;
  _created_timestamp: Date;
};

class DamaEvents extends DamaContextAttachedResource {
  protected readonly pglisten_subscribers_by_pgenv: Record<
    PgEnv,
    Promise<PgListenSubscriber> | undefined
  >;

  protected readonly final_event_listeners_by_etl_context_by_pg_env: Record<
    PgEnv,
    Record<number, Array<(final_event: DamaEvent) => any>>
  >;

  constructor() {
    super();
    this.pglisten_subscribers_by_pgenv = {};
    this.final_event_listeners_by_etl_context_by_pg_env = {};
  }

  async spawnEtlContext(
    source_id: number | null = null,
    parent_context_id: number | null = null
  ): Promise<number> {
    this.logger.silly(
      `dama_events.spawnEtlContext: source_id=${source_id}, parent_context_id=${parent_context_id}`
    );

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
    this.logger.silly(
      `dama_events.setEtlContextSourceId: etl_context_id=${etl_context_id}, source_id=${source_id}`
    );

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

    this.logger.debug(
      `dama_events dispatched event: ${JSON.stringify(dama_event)}`
    );

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

  async getInitialEvent(
    etl_context_id = this.etl_context_id,
    pg_env = this.pg_env
  ) {
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
    } = await dama_db.query(
      {
        text,
        values: [etl_context_id],
      },
      pg_env
    );

    if (!initial_event) {
      throw new Error(`No :INITIAL event for EtlContext ${etl_context_id}`);
    }

    return initial_event;
  }

  async getEtlContextFinalEvent(
    etl_context_id = this.etl_context_id,
    pg_env = this.pg_env
  ) {
    const text = dedent(`
      SELECT
          b.*
        FROM data_manager.etl_contexts AS a
          INNER JOIN data_manager.event_store AS b
            ON ( a.latest_event_id = b.event_id )
        WHERE (
          ( a.etl_context_id = $1 )
          AND
          ( a.etl_status = 'DONE' )
        )
      ;
    `);

    const {
      rows: [final_event],
    } = await dama_db.query(
      {
        text,
        values: [etl_context_id],
      },
      pg_env
    );

    if (!final_event) {
      throw new Error(`No :FINAL event for EtlContext ${etl_context_id}`);
    }

    return final_event;
  }

  async registerEtlContextFinalEventListener(
    etl_context_id: number,
    fn: (final_event: DamaEvent) => any,
    pg_env = this.pg_env
  ) {
    try {
      // If the EtlContext is already done, there will be no new NOTIFY.
      // Therefore, we must immediately call with the :FINAL event.
      const final_event = await this.getEtlContextFinalEvent(
        etl_context_id,
        pg_env
      );

      process.nextTick(() => fn(final_event));
    } catch (err) {
      // Note:  registering the listener happens synchronously so no race condition
      //        even if the listener already exists.
      this.final_event_listeners_by_etl_context_by_pg_env[pg_env] =
        this.final_event_listeners_by_etl_context_by_pg_env[pg_env] || {};

      this.final_event_listeners_by_etl_context_by_pg_env[pg_env][
        etl_context_id
      ] =
        this.final_event_listeners_by_etl_context_by_pg_env[pg_env][
          etl_context_id
        ] || [];

      this.final_event_listeners_by_etl_context_by_pg_env[pg_env][
        etl_context_id
      ].push(fn);

      await this.addPgListenSubscriber(pg_env);
    }
  }

  protected async addPgListenSubscriber(
    pg_env = this.pg_env
  ): Promise<PgListenSubscriber> {
    // IDEMPOTENCY: If there is Promise assigned for this PgEnv, we've already started initialization.
    if (this.pglisten_subscribers_by_pgenv[pg_env]) {
      return <Promise<PgListenSubscriber>>(
        this.pglisten_subscribers_by_pgenv[pg_env]
      );
    }

    let done: (subscriber: PgListenSubscriber) => PgListenSubscriber;
    let fail: Function;

    this.pglisten_subscribers_by_pgenv[pg_env] = new Promise(
      (resolve, reject) => {
        // @ts-ignore
        done = resolve;
        fail = reject;
      }
    );

    const notifyListeners = async (etl_context_id: number | string) => {
      this.logger.silly(
        `dama_events.notifyListeners etl_context_id=${etl_context_id} start`
      );

      etl_context_id = +etl_context_id;

      try {
        let listeners =
          this.final_event_listeners_by_etl_context_by_pg_env[pg_env]?.[
            etl_context_id
          ];

        if (!listeners) {
          this.logger.silly(
            `dama_events.notifyListeners etl_context_id=${etl_context_id} has no listeners`
          );

          return;
        }

        // Throws if no :FINAL event.
        const final_event = await this.getEtlContextFinalEvent(
          etl_context_id,
          pg_env
        );

        this.logger.silly(
          `dama_events.notifyListeners etl_context_id=${etl_context_id} final_event=${JSON.stringify(
            final_event,
            null,
            4
          )}`
        );

        // NOTE:  At this point, if a new listener is registered,
        //        the registerEtlContextDoneListener method calls that listener.
        listeners =
          this.final_event_listeners_by_etl_context_by_pg_env[pg_env]?.[
            etl_context_id
          ];

        // deleting the listeners array (?SHOULD?) guarantee listeners called ONLY ONCE.
        delete this.final_event_listeners_by_etl_context_by_pg_env[pg_env]?.[
          etl_context_id
        ];

        if (listeners) {
          this.logger.silly(
            `dama_events.notifyListeners etl_context_id=${etl_context_id} notifying listeners`
          );
          await Promise.all(listeners.map((fn) => fn(final_event)));
        }
      } catch (err) {
        if (/^No :FINAL event for EtlContext/.test(err.message)) {
          this.logger.silly(
            `dama_events.notifyListeners etl_context_id=${etl_context_id} no :FINAL event`
          );
        } else {
          this.logger.error(err.message);
        }
      }
    };

    try {
      const subscriber = createPostgresSubscriber({
        connectionString: getPostgresConnectionUri(pg_env),
      });

      subscriber.notifications.on("ETL_CONTEXT_FINAL_EVENT", notifyListeners);

      //  NOTE: The following sequence will cause a missed :FINAL event
      //
      //    t0       t1        t2     t3
      //    connect, register, event, listen
      //
      await subscriber.connect();
      await subscriber.listenTo("ETL_CONTEXT_FINAL_EVENT");

      // Handle the potentially missed :FINAL event case described above.
      process.nextTick(async () => {
        if (!this.final_event_listeners_by_etl_context_by_pg_env[pg_env]) {
          return;
        }

        const final_event_listeners_by_etl_context =
          this.final_event_listeners_by_etl_context_by_pg_env[pg_env];

        const etl_context_ids = Object.keys(
          final_event_listeners_by_etl_context
        );

        await Promise.all(etl_context_ids.map(notifyListeners));
      });

      process.nextTick(() => done(subscriber));

      return subscriber;
    } catch (err) {
      process.nextTick(() => fail(err));
      throw err;
    }
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
