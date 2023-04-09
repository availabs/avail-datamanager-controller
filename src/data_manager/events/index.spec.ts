import { v4 as uuid } from "uuid";

import dama_db from "../dama_db";
import dama_events from ".";

const PG_ENV = "test_db";

test("spawns new EtlContext", async () => {
  const all_etl_context_ids_sql = `
    SELECT
        array_agg(etl_context_id) AS etl_context_ids
      FROM data_manager.etl_contexts
  `;

  let {
    rows: [{ etl_context_ids: initial_etl_context_ids }],
  } = await dama_db.query(all_etl_context_ids_sql, PG_ENV);

  initial_etl_context_ids = initial_etl_context_ids || [];

  const etl_context_id = await dama_events.spawnEtlContext(null, null, PG_ENV);

  const {
    rows: [{ etl_context_ids: subsequent_etl_context_ids }],
  } = await dama_db.query(all_etl_context_ids_sql, PG_ENV);

  expect(initial_etl_context_ids.includes(etl_context_id)).toBe(false);

  expect(subsequent_etl_context_ids.includes(etl_context_id)).toBe(true);
});

test("set EtlContext DamaSourceID", async () => {
  const source_name = uuid().slice(0, 10);

  const {
    rows: [{ source_id }],
  } = await dama_db.query(
    {
      text: `
        INSERT INTO data_manager.sources ( name )
          VALUES ( $1 )
          RETURNING source_id
      `,
      values: [source_name],
    },
    PG_ENV
  );

  const etl_context_id = await dama_events.spawnEtlContext(null, null, PG_ENV);

  await dama_events.setEtlContextSourceId(etl_context_id, source_id, PG_ENV);

  const {
    rows: [{ exists }],
  } = await dama_db.query(
    {
      text: `
        SELECT EXISTS (
          SELECT
              1
            FROM data_manager.etl_contexts AS a
              INNER JOIN data_manager.sources AS b
                USING ( source_id )
            WHERE (
              ( etl_context_id = $1 )
              AND
              ( source_id = $2 )
            )
        ) AS exists
      `,
      values: [etl_context_id, source_id],
    },
    PG_ENV
  );

  expect(exists).toBe(true);
});

test("dispatches :INITIAL and :FINAL events", async () => {
  const etl_context_id = await dama_events.spawnEtlContext(null, null, PG_ENV);

  const initial_event = {
    type: ":INITIAL",
    payload: { id: uuid() },
  };

  await dama_events.dispatch(initial_event, etl_context_id, PG_ENV);

  const final_event = {
    type: ":FINAL",
    payload: { id: uuid() },
  };

  await dama_events.dispatch(final_event, etl_context_id, PG_ENV);

  const { rows: events_in_db } = await dama_db.query(
    {
      text: `
        SELECT
            type,
            payload
          FROM data_manager.event_store
          WHERE ( etl_context_id = $1 )
          ORDER BY event_id
      `,
      values: [etl_context_id],
    },
    PG_ENV
  );

  expect(events_in_db.length).toBe(2);

  expect(events_in_db[0]).toEqual(initial_event);
  expect(events_in_db[1]).toEqual(final_event);
});

test("first dispatched event MUST be :INITIAL", async () => {
  const etl_context_id = await dama_events.spawnEtlContext(null, null, PG_ENV);

  const invalid_first_event = {
    type: ":INVALID",
    payload: { id: uuid() },
  };

  const initial_event = {
    type: ":INITIAL",
    payload: { id: uuid() },
  };

  expect(
    dama_events.dispatch(invalid_first_event, etl_context_id, PG_ENV)
  ).rejects.toThrow();

  await dama_events.dispatch(initial_event, etl_context_id, PG_ENV);

  await dama_events.dispatch(invalid_first_event, etl_context_id, PG_ENV);

  const { rows: events_in_db } = await dama_db.query(
    {
      text: `
        SELECT
            type,
            payload
          FROM data_manager.event_store
          WHERE ( etl_context_id = $1 )
          ORDER BY event_id
      `,
      values: [etl_context_id],
    },
    PG_ENV
  );
  expect(events_in_db.length).toBe(2);

  expect(events_in_db[0]).toEqual(initial_event);
  expect(events_in_db[1]).toEqual(invalid_first_event);
});

test("last dispatched event MUST be :FINAL", async () => {
  const etl_context_id = await dama_events.spawnEtlContext(null, null, PG_ENV);

  const initial_event = {
    type: ":INITIAL",
    payload: { id: uuid() },
  };

  const final_event = {
    type: ":FINAL",
    payload: { id: uuid() },
  };

  const invalid_last_event = {
    type: ":INVALID",
    payload: { id: uuid() },
  };

  await dama_events.dispatch(initial_event, etl_context_id, PG_ENV);
  await dama_events.dispatch(final_event, etl_context_id, PG_ENV);

  expect(
    dama_events.dispatch(invalid_last_event, etl_context_id, PG_ENV)
  ).rejects.toThrow();

  const { rows: events_in_db } = await dama_db.query(
    {
      text: `
        SELECT
            type,
            payload
          FROM data_manager.event_store
          WHERE ( etl_context_id = $1 )
          ORDER BY event_id
      `,
      values: [etl_context_id],
    },
    PG_ENV
  );

  expect(events_in_db.length).toBe(2);

  expect(events_in_db[0]).toEqual(initial_event);
  expect(events_in_db[1]).toEqual(final_event);
});

test("events dispatched from within dama_db TransactionContext get rolled back on Error", async () => {
  const etl_context_id = await dama_events.spawnEtlContext(null, null, PG_ENV);

  const initial_event = {
    type: ":INITIAL",
    payload: { id: uuid() },
  };

  const checkpoint_event = {
    type: ":CHECKPOINT",
    payload: { id: uuid() },
  };

  await dama_events.dispatch(initial_event, etl_context_id, PG_ENV);

  // Since all work that happens during a TRANSACTION gets ROLLED BACK,
  // we need to make sure any checkpoint events get rolled back as well.
  // Otherwise, checkpoints would be unreliale and retried Tasks would
  // skip work that needs to be redone.
  expect(
    dama_db.runInTransactionContext(async () => {
      await dama_events.dispatch(checkpoint_event, etl_context_id, PG_ENV);

      throw new Error("ETL ERROR");
    }, PG_ENV)
  ).rejects.toThrow("ETL ERROR");

  const { rows: events_in_db } = await dama_db.query(
    {
      text: `
        SELECT
            type,
            payload
          FROM data_manager.event_store
          WHERE ( etl_context_id = $1 )
          ORDER BY event_id
      `,
      values: [etl_context_id],
    },
    PG_ENV
  );

  expect(events_in_db.length).toBe(1);

  expect(events_in_db[0]).toEqual(initial_event);
});
