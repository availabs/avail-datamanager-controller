/* eslint-disable max-classes-per-file */

import dedent from "dedent";
import pgFormat from "pg-format";
import _ from "lodash";

import createPostgresSubscriber, {
  Subscriber as PgListenSubscriber,
} from "pg-listen";

import DamaContextAttachedResource, { EtlContextId } from "../contexts";

import dama_db from "../dama_db";
import logger from "../logger";

import {
  PgEnv,
  getPostgresConnectionUri,
} from "../dama_db/postgres/PostgreSQL";

type EtlNonErrorEvent = {
  type: string;
  payload?: Record<string, any> | null;
  meta?: Record<string, any> | null;
  error?: false | null;
};

type EtlErrorEvent = {
  type: string;
  payload: any;
  meta?: Record<string, any> | null;
  error: true;
};

export type EtlEvent = EtlNonErrorEvent | EtlErrorEvent;

export type DamaEvent = EtlEvent & {
  event_id: number;
  etl_context_id: number;
  _created_timestamp: Date;
};

class NoFinalEventForEtlContextError extends Error {
  constructor(etl_context_id: number) {
    super(`No :FINAL event for EtlContext ${etl_context_id}`);
    this.name = "NoFinalEventForEtlContextError";
  }
}

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

  /**
   * Creates a new EtlContext.
   *
   * @remarks
   *  INSERTs a row into the data_manager.etl_contexts TABLE and returns the etl_context_id.
   *  NOTE: An EtlContext is REQUIRED to dispatch events.
   *
   * @param source_id - The DamaSource ID.
   *
   * @param parent_context_id - The ID of the EtlContext within which this context is being spawned.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns the ID of the spawned EtlContext
   */
  async spawnEtlContext(
    source_id: number | null = null,
    parent_context_id: EtlContextId | null = null,
    pg_env = this.pg_env
  ): Promise<EtlContextId> {
    logger.silly(
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
      rows: [{ etl_context_id: new_etl_context_id }],
    } = await dama_db.query(
      {
        text: sql,
        values: [parent_context_id, source_id],
      },
      pg_env
    );

    return new_etl_context_id;
  }

  /**
   * Get the parent_context_id for an etl_context_id.
   *
   * @param etl_context_id - The ID of the EtlContext. Optional if running in a dama_context.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context.
   *
   * @returns the ID of the parent EtlContext, or NULL if none exists.
   */
  async getParentEtlContextId(
    etl_context_id = this.etl_context_id,
    pg_env = this.pg_env
  ): Promise<EtlContextId> {
    const sql = `
      SELECT
          parent_context_id
        FROM data_manager.etl_contexts
        WHERE ( etl_context_id = $1 )
    `;

    const { rows } = await dama_db.query(
      {
        text: sql,
        values: [etl_context_id],
      },
      pg_env
    );

    if (rows.length === 0) {
      throw new Error(
        `No such EtlContext: pg_env=${pg_env}, etl_context_id=${etl_context_id}`
      );
    }

    const [{ parent_context_id }] = rows;

    return parent_context_id;
  }

  /**
   * Set the DamaSourceID for an existing EtlContext.
   *
   * @param etl_context_id - The EtlContext ID. Optional if running in a dama_context EtlContext.
   *
   * @param source_id - The DamaSource ID.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   */
  async setEtlContextSourceId(
    etl_context_id: number,
    source_id: number,
    pg_env = this.pg_env
  ) {
    logger.silly(
      `dama_events.setEtlContextSourceId: etl_context_id=${etl_context_id}, source_id=${source_id}`
    );

    const sql = `
      UPDATE data_manager.etl_contexts
        SET source_id = $1
        WHERE etl_context_id = $2
    `;

    await dama_db.query(
      {
        text: sql,
        values: [source_id, etl_context_id],
      },
      pg_env
    );
  }

  /**
   * Set the DamaSourceID for an existing EtlContext.
   *
   * @param etl_context_id - The EtlContext ID. Optional if running in a dama_context EtlContext.
   *
   * @param source_id - The DamaSource ID.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   */
  async dispatch(
    event: EtlEvent,
    etl_context_id = this.etl_context_id,
    pg_env = this.pg_env
  ): Promise<DamaEvent> {
    const { type, meta = null, error = null } = event;

    let { payload = null } = event;

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
    } = await dama_db.query(
      {
        text: sql,
        values,
      },
      pg_env
    );

    logger.debug(`dama_events dispatched event: ${JSON.stringify(dama_event)}`);

    return dama_event;
  }

  /**
   * Queries all events after since_event_id for the passes etl_context_id for the EtlContext tree.
   * The EtlContext tree includes the EtlContext, as well as all ancestors and descendents.
   *
   * @remarks
   *    NOTE: Queries all events in the EtlContext tree... all ancestors and descendants.
   *        TODO: Make this behavior configurable.
   *
   * @param since_event_id - Return all events in the EtlContext tree with
   *    event_id greater than the  since_event_id.
   *
   * @param etl_context_id - The ID of the EtlContext whose ancestors', self's,
   *    and descendants' events to query. Optional if in a dama_context EtlContext.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns An array of all the events that occurred in the EtlContext tree after since_event_id.
   */
  async queryEvents(
    since_event_id = -1,
    etl_context_id = this.etl_context_id,
    pg_env = this.pg_env
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

    const { rows: dama_events } = await dama_db.query(
      {
        text: sql,
        values: [etl_context_id, since_event_id],
      },
      pg_env
    );

    // FIXME: Why are we doing this? Can it be deprecated without breaking anything?
    dama_events.forEach((e: DamaEvent) => {
      e.meta = e.meta || {};
      e.meta.pgEnv = this.pg_env;
    });

    return dama_events;
  }

  /**
   * Returns the :INITIAL event for the EtlContext.
   *
   * @param etl_context_id - The EtlContext ID. Optional if running in a dama_context EtlContext.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns A Promise for the :INITIAL event.
   *
   * @throws Will throw if no :INITIAL event found for the the etl_context_id.
   */
  async getEtlContextInitialEvent(
    etl_context_id = this.etl_context_id,
    pg_env = this.pg_env
  ): Promise<DamaEvent> {
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

  /**
   * Returns the :FINAL event for the EtlContext.
   *
   * @remarks
   *    NOTE: This method throws if the :FINAL event does not exist.
   *    If you wish to await the :FINAL event, use getEventualEtlContextFinalEvent
   *
   * @param etl_context_id - The EtlContext ID. Optional if running in a dama_context EtlContext.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns A Promise for the :FINAL event.
   *
   * @throws Will throw if no :FINAL event found for the the etl_context_id.
   */
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
      throw new NoFinalEventForEtlContextError(<number>etl_context_id);
    }

    return final_event;
  }

  /**
   * Returns a Promise for an EtlContext's :FINAL event.
   *   If the :FINAL event already exists, the Promise immediately resolves with the event.
   *   If the :FINAL event does not yet exists, the Promise will resolve when
   *     the :FINAL event is written to the database.
   *
   * @remarks
   *    Uses registerEtlContextFinalEventListener.
   *    NOTE: This method may need to wait for a long-running task to finish.
   *          The getEtlContextFinalEvent does not wait for tasks.
   *            It will throw an error if the :FINAL event does not exist.
   *
   * @param etl_context_id - The EtlContext ID whose :FINAL event will be passed to the listener.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   */
  getEventualEtlContextFinalEvent(
    etl_context_id: number,
    pg_env = this.pg_env
  ) {
    return new Promise<DamaEvent>((resolve) =>
      this.registerEtlContextFinalEventListener(etl_context_id, resolve, pg_env)
    );
  }

  /**
   * Private method used to notify :FINAL event listeners.
   *
   * @param etl_context_id - The EtlContext ID whose :FINAL event will be passed to the listener.
   *
   * @param pg_env - The database to connect to.
   *
   * @param log_missing_events - Whether to log error messages when the :FINAL event
   *    cannot be found in data_manager.event_store.
   */
  private async notifyFinalEventListeners(
    etl_context_id: number | string,
    pg_env: string,
    log_missing_events = true
  ) {
    etl_context_id = +etl_context_id;

    logger.silly(
      `dama_events.notifyFinalEventListeners etl_context_id=${etl_context_id} start`
    );

    try {
      let listeners =
        this.final_event_listeners_by_etl_context_by_pg_env[pg_env]?.[
          etl_context_id
        ];

      // If this (pg_env, etl_context_id) does not have any :FINAL event listeners, we're done.
      if (!listeners) {
        return;
      }

      // NOTE: Throws if no :FINAL event. So, if no :FINAL event, we go to the catch block.
      const final_event = await this.getEtlContextFinalEvent(
        etl_context_id,
        pg_env
      );

      // If we have reached this point, the EtlContext has a :FINAL event.

      logger.silly(
        `dama_events.notifyFinalEventListeners etl_context_id=${etl_context_id} final_event=${JSON.stringify(
          final_event,
          null,
          4
        )}`
      );

      // NOTE: We refresh the listeners array in case one was added
      //        during the async getEtlContextFinalEvent call
      //
      // NOTE:  The following two synchronous lines get the listeners, then unregister them.
      //
      //        While they are executing, because Node is single-threaded,
      //          no new listeners can be registered.
      //
      //        If a new listener is registered after they finish,
      //          the registerEtlContextDoneListener method calls that listener
      //          because it bypasses the Postgres NOTIFY/LISTEN mechanism
      //          and calls the listener immediately if the :FINAL event already exists.
      listeners =
        this.final_event_listeners_by_etl_context_by_pg_env[pg_env]?.[
          etl_context_id
        ];

      // deleting the listeners array (?SHOULD?) guarantee listeners called ONLY ONCE.
      delete this.final_event_listeners_by_etl_context_by_pg_env[pg_env]?.[
        etl_context_id
      ];

      if (listeners) {
        // Call all the listeners.
        //
        // NOTE: Could interleave listeners. May be better to use a for loop and call them sequentially.
        //        That would be close to how Node's EventEmitter works.
        await Promise.all(
          listeners.map((listener) => {
            try {
              return listener(final_event);
            } catch (err) {
              // CONISIDER: The listener should catch this Error?
              //            If we do not discard it here, other listeners will not get notified.
              logger.warn(
                `dama_events.notifyFinalEventListeners ignoring error for pg_env=${pg_env} etl_context_id=${etl_context_id}`
              );
              logger.warn((<Error>err).message);
              logger.warn((<Error>err).stack);
            }
          })
        );
      }
    } catch (err) {
      if (err instanceof NoFinalEventForEtlContextError) {
        if (log_missing_events) {
          logger.warn(
            `dama_events.notifyFinalEventListeners pg_env=${pg_env} etl_context_id=${etl_context_id} no :FINAL event`
          );
        }
      } else {
        logger.warn(
          `dama_events.notifyFinalEventListeners ignoring error for pg_env=${pg_env} etl_context_id=${etl_context_id}`
        );
        logger.error((<Error>err).message);
        logger.error((<Error>err).stack);
      }
    }
  }

  /**
   * Register a listener for an EtlContext's :FINAL event.
   *   If the :FINAL event already exists, the listener is immediately called with the event.
   *   If the :FINAL event does not yet exists, the listener will be called when
   *     the :FINAL event is written to the database.
   *
   * @remarks
   *   Uses PostgreSQL's NOTIFY/LISTEN
   *     * https://www.postgresql.org/docs/current/sql-notify.html
   *     * https://github.com/andywer/pg-listen
   *
   * @param etl_context_id - The EtlContext ID whose :FINAL event will be passed to the listener.
   *
   * @param listener - Function that will be called with the :FINAL event when it occurs.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   */
  async registerEtlContextFinalEventListener(
    etl_context_id: number,
    listener: (final_event: DamaEvent) => any,
    pg_env = this.pg_env
  ) {
    try {
      // If the EtlContext is already done, there will be no new NOTIFY.
      // Therefore, we must immediately call with the :FINAL event.
      // getEtlContextFinalEvent throws if no :FINAL event.
      const final_event = await this.getEtlContextFinalEvent(
        etl_context_id,
        pg_env
      );

      return process.nextTick(() => listener(final_event));
    } catch (err) {
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
      ].push(listener);

      await this.addPgListenSubscriber(pg_env);
    }
  }

  /**
   * Listen for :FINAL event notifications from the database.
   *
   * @remarks
   *   NOTIFY happens in an INSERT TRIGGER on data_manager.event_store.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   */
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

    try {
      const subscriber = createPostgresSubscriber({
        connectionString: getPostgresConnectionUri(pg_env),
      });

      // Will call notifyFinalEventListeners when an EtlContext gets a :FINAL event.
      // NOTE: ETL_CONTEXT_FINAL_EVENT event is fired by pg_notify in
      //       data_manager.event_store_etl_context_status_update_fn TRIGGER FUNCTION.
      subscriber.notifications.on("ETL_CONTEXT_FINAL_EVENT", (eci) =>
        this.notifyFinalEventListeners(eci, pg_env, true)
      );

      //  NOTE: The following async sequence would cause the subscriber to miss a :FINAL event
      //
      //          t0       t1        t2     t3
      //          connect, register, event, listen
      //
      //        Therefore, after we start listening, we call directly call notifyFinalEventListeners
      //          outside of the subscriber.notifications callback.
      //
      //        If the :FINAL event exists in the data_manager.event_store,
      //          it'll notify the listeners and unregister them.
      //
      //        If the :FINAL event does not exist, it is essentially a no-op, which should ensure
      //          listeners get called ONCE.
      //
      await subscriber.connect();
      await subscriber.listenTo("ETL_CONTEXT_FINAL_EVENT");

      // I believe this handles the potentially missed :FINAL event case described above.
      process.nextTick(async () => {
        const final_event_listeners_by_etl_context =
          this.final_event_listeners_by_etl_context_by_pg_env[pg_env];

        if (!final_event_listeners_by_etl_context) {
          return;
        }

        const etl_context_ids = Object.keys(
          final_event_listeners_by_etl_context
        );

        //  Calling notifyFinalEventListeners outside of subscriber.notifications so that
        //    if we missed the :FINAL event because it happened while we were subscribing to it,
        //    the listeners will still be notified.
        //
        //  NOTE: notifyFinalEventListeners unregisters listeners before they are called
        //        so it should be safe to call multiple times.
        await Promise.all(
          etl_context_ids.map((eci) =>
            this.notifyFinalEventListeners(eci, pg_env, false)
          )
        );
      });

      process.nextTick(() => done(subscriber));

      return subscriber;
    } catch (err) {
      process.nextTick(() => fail(err));
      throw err;
    }
  }

  /**
   * Get all the events for an EtlContext.
   *
   * @param etl_context_id - The EtlContext ID. Optional if running in a dama_context EtlContext.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   */
  async getAllEtlContextEvents(
    etl_context_id = this.etl_context_id,
    pg_env = this.pg_env
  ) {
    const text = dedent(`
      SELECT
          a.*
        FROM data_manager.event_store AS a
        WHERE ( etl_context_id = $1 )
        ORDER BY event_id
      ;
    `);

    const { rows: events } = await dama_db.query(
      {
        text,
        values: [etl_context_id],
      },
      pg_env
    );

    return events;
  }

  /**
   * Get all the latest events for OPEN EtlContexts for a given DamaSourceType.
   *
   * @remarks
   *    EtlContexts can be in three states: OPEN, ERROR, and DONE.
   *
   * @param data_type - The DamaType. (data_manager.sources.type column).
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns An Array constaining the latest event for each OPEN EtlContext.
   */
  async queryOpenEtlProcessesLatestEventForDataSourceType(
    data_type: string,
    pg_env = this.pg_env
  ): Promise<DamaEvent[]> {
    const q = dedent(
      pgFormat(
        `
          SELECT
              c.*
            FROM data_manager.sources AS a
              INNER JOIN data_manager.etl_contexts AS b
                ON (
                  ( a.type = $1 )
                  AND
                  ( b.etl_status = 'OPEN' )
                  AND
                  ( a.source_id = b.source_id)
                )
              INNER JOIN data_manager.event_store AS c
                ON (
                  ( b.etl_context_id = c.etl_context_id )
                  AND
                  ( b.latest_event_id = c.event_id )
                )
            ORDER BY c.event_id
          ;
        `
      )
    );

    const { rows } = await dama_db.query(
      { text: q, values: [data_type] },
      pg_env
    );

    return rows;
  }

  /**
   * Get all the latest events for non-OPEN (ERROR or DONE etl_status) EtlContexts for a given DamaSourceType.
   *
   * @remarks
   *    EtlContexts can be in three states: OPEN, ERROR, and DONE.
   *
   * @param data_type - The DamaType. (data_manager.sources.type column).
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns An Array constaining the latest event for each EtlContext with elt_status ERROR or DONE.
   */
  async queryNonOpenEtlProcessesLatestEventForDataSourceType(
    data_type: string,
    pg_env = this.pg_env
  ): Promise<DamaEvent[]> {
    const q = dedent(
      pgFormat(
        `
          SELECT
              c.*
            FROM data_manager.sources AS a
              INNER JOIN data_manager.etl_contexts AS b
                ON (
                  ( a.type = $1 )
                  AND
                  ( b.etl_status <> 'OPEN' )
                  AND
                  ( a.source_id = b.source_id)
                )
              INNER JOIN data_manager.event_store AS c
                ON (
                  ( b.etl_context_id = c.etl_context_id )
                  AND
                  ( b.latest_event_id = c.event_id )
                )
            ORDER BY c.event_id
          ;
        `
      )
    );

    const { rows } = await dama_db.query(
      { text: q, values: [data_type] },
      pg_env
    );

    return rows;
  }

  /**
   * DEPRECATED: Get all the OPEN EtlContexts for a given Moleculer Service.
   *
   * @deprecated Use queryOpenEtlProcessesLatestEventForDataSourceType instead.
   *
   * @remarks
   *    EtlContexts can be in three states: OPEN, ERROR, and DONE.
   *    FIXME: The query is outdated. Use the etl_contexts table's etl_status and latest_event_id columns.
   *
   * @param service_name - The Moleculer Service name.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns An Array constaining the latest event for each OPEN EtlContext.
   */
  async queryOpenEtlProcessesStatusUpdatesForService(
    service_name: string,
    pg_env = this.pg_env
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

    const { rows } = await dama_db.query(q, pg_env);

    if (rows.length === 0) {
      return [];
    }

    return rows.map((row) => <DamaEvent>_.omit(row, "row_number"));
  }

  /**
   * DEPRECATED: Get latest event for non-OPEN EtlContexts.
   *
   * @deprecated Use queryNonOpenEtlProcessesLatestEventForDataSourceType instead.
   *
   * @remarks
   *    EtlContexts can be in three states: OPEN, ERROR, and DONE.
   *
   *    FIXME:
   *      * The query is outdated. Use the etl_contexts table's etl_status and latest_event_id columns.
   *      * Wrong meaning for "Final". Rename the method.
   *      * ASSUMES the following CONVENTION/INVARIANTs:
   *          1. Event type prefixed by service_name. E.G:  `${service_name}/foo/bar:BAZ`
   *          2. All ETL processes for the service end with a ":FINAL" or ":ERROR" event
   *          3. All status update types match with /*UPDATE$/
   *
   *        These ASSUMPTIONS are no longer valid since Tasks support retry.
   *          TODO: Look at the repercussions of changing logic.
   *
   * @param etl_context_id - The EtlContext ID. Optional if running in a dama_context EtlContext.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns An the latest event for the EtlContext.
   */
  async queryEtlContextFinalEvent(
    etl_context_id = this.etl_context_id,
    pg_env = this.pg_env
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

    const { rows } = await dama_db.query(
      {
        text: q,
        values: [etl_context_id],
      },
      pg_env
    );

    if (rows.length === 0) {
      return null;
    }

    return rows[0];
  }

  /**
   * Shutdown the dama_events service.
   *
   * @remarks
   *    The Node.js process will hang if listeners have been added and this method is not called.
   */
  async shutdown() {
    for (const pg_env of Object.keys(this.pglisten_subscribers_by_pgenv)) {
      const subscriber = await this.pglisten_subscribers_by_pgenv[pg_env];

      // eslint-disable-next-line no-unused-expressions
      subscriber?.close();
    }
  }
}

export default new DamaEvents();
