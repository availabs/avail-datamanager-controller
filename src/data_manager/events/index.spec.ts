import { v4 as uuid } from "uuid";
import _ from "lodash";

import dama_db from "../dama_db";
import dama_events, { DamaEvent } from ".";

const PG_ENV = "ephemeral_test_db";

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

test("EtlContexts can have ONLY ONE :INITIAL event", async () => {
  const etl_context_id = await dama_events.spawnEtlContext(null, null, PG_ENV);

  const initial_event = {
    type: ":INITIAL",
    payload: { id: uuid() },
  };

  await dama_events.dispatch(initial_event, etl_context_id, PG_ENV);

  expect(
    dama_events.dispatch(initial_event, etl_context_id, PG_ENV)
  ).rejects.toThrow();
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

test("listeners for :FINAL events called ONLY when :FINAL event COMMITTED to database", async () => {
  const id_a = uuid();
  const id_b = uuid();

  const initial_event = {
    type: ":INITIAL",
  };

  const final_event_a = {
    type: ":FINAL",
    payload: { id: id_a },
  };

  const final_event_b = {
    type: ":FINAL",
    payload: { id: id_b },
  };

  const eci_a = await dama_events.spawnEtlContext(null, null, PG_ENV);
  const eci_b = await dama_events.spawnEtlContext(null, null, PG_ENV);

  let was_notified_a = false;
  let was_notified_b = false;

  await dama_events.dispatch(initial_event, eci_a, PG_ENV);
  await dama_events.dispatch(initial_event, eci_b, PG_ENV);

  dama_events.registerEtlContextFinalEventListener(
    eci_a,
    (e) => {
      was_notified_a = true;
      expect(e.payload.id).toBe(id_a);
    },
    PG_ENV
  );

  dama_events.registerEtlContextFinalEventListener(
    eci_b,
    (e) => {
      was_notified_b = true;
      expect(e.payload.id).toBe(id_b);
    },
    PG_ENV
  );

  await dama_db.runInTransactionContext(async () => {
    await dama_events.dispatch(final_event_a, eci_a);
  }, PG_ENV);

  expect(
    dama_db.runInTransactionContext(async () => {
      await dama_events.dispatch(final_event_b, eci_b);

      throw new Error("ETL ERROR");
    }, PG_ENV)
  ).rejects.toThrow("ETL ERROR");

  await new Promise((r) => setTimeout(r, 1000));

  expect(was_notified_a).toBe(true);
  expect(was_notified_b).toBe(false);
});

test("gets all events for an EtlContext", async () => {
  const N = 10;

  const initial_event = {
    type: ":INITIAL",
  };

  const final_event = {
    type: ":FINAL",
  };

  const eci = await dama_events.spawnEtlContext(null, null, PG_ENV);

  await dama_events.dispatch(initial_event, eci, PG_ENV);

  const facts = _.range(0, N);

  for (const fact of facts) {
    await dama_events.dispatch(
      {
        type: ":FACT",
        payload: { fact },
      },
      eci,
      PG_ENV
    );
  }

  await dama_events.dispatch(final_event, eci, PG_ENV);

  const all_events = await dama_events.getAllEtlContextEvents(eci, PG_ENV);

  expect(all_events.length).toBe(N + 2);

  // NOTE: all_events is sorted by event_id
  const event_facts = all_events
    .map((e) => e?.payload?.fact)
    .filter((f) => Number.isFinite(f));

  expect(event_facts).toEqual(facts);
});

test("queryOpenEtlProcessesLatestEventForDataSourceType", async () => {
  const N = 10;

  const source_name = uuid().slice(0, 10);
  const type = uuid();

  const {
    rows: [{ source_id }],
  } = await dama_db.query(
    {
      text: `
        INSERT INTO data_manager.sources ( name, type )
          VALUES ( $1, $2 )
          RETURNING source_id
      `,
      values: [source_name, type],
    },
    PG_ENV
  );

  const initial_event = {
    type: ":INITIAL",
  };

  const final_event = {
    type: ":FINAL",
  };

  const eci_a = await dama_events.spawnEtlContext(source_id, null, PG_ENV);
  const eci_b = await dama_events.spawnEtlContext(source_id, null, PG_ENV);

  let prev_latest: DamaEvent[];
  let cur_latest: DamaEvent[];

  cur_latest =
    await dama_events.queryOpenEtlProcessesLatestEventForDataSourceType(
      type,
      PG_ENV
    );

  expect(Array.isArray(cur_latest)).toBe(true);
  expect(cur_latest.length).toBe(0);

  prev_latest = cur_latest;

  await dama_events.dispatch(initial_event, eci_a, PG_ENV);

  cur_latest =
    await dama_events.queryOpenEtlProcessesLatestEventForDataSourceType(
      type,
      PG_ENV
    );

  expect(cur_latest.length).toBe(1);
  expect(cur_latest[0].etl_context_id).toBe(eci_a);

  prev_latest = cur_latest;

  await dama_events.dispatch(initial_event, eci_b, PG_ENV);

  cur_latest =
    await dama_events.queryOpenEtlProcessesLatestEventForDataSourceType(
      type,
      PG_ENV
    );

  expect(cur_latest.length).toBe(2);
  expect(cur_latest[0].etl_context_id).toBe(eci_a);
  expect(cur_latest[1].etl_context_id).toBe(eci_b);

  prev_latest = cur_latest;

  const facts = _.range(0, N);

  for (const fact of facts) {
    await dama_events.dispatch(
      {
        type: ":FACT",
        payload: { fact },
      },
      eci_a,
      PG_ENV
    );

    cur_latest =
      await dama_events.queryOpenEtlProcessesLatestEventForDataSourceType(
        type,
        PG_ENV
      );

    expect(cur_latest[0]).toEqual(prev_latest[1]);
    expect(cur_latest[1].etl_context_id).toEqual(eci_a);
    expect(cur_latest[1].payload.fact).toEqual(fact);

    await dama_events.dispatch(
      {
        type: ":FACT",
        payload: { fact },
      },
      eci_b,
      PG_ENV
    );

    cur_latest =
      await dama_events.queryOpenEtlProcessesLatestEventForDataSourceType(
        type,
        PG_ENV
      );

    expect(cur_latest[0].etl_context_id).toEqual(eci_a);
    expect(cur_latest[0].payload.fact).toEqual(fact);

    expect(cur_latest[1].etl_context_id).toEqual(eci_b);
    expect(cur_latest[1].payload.fact).toEqual(fact);

    prev_latest = cur_latest;
  }

  // After dispatching a :FINAL events, the EtlContext is no longer 'OPEN'.
  //   Therefore, queryOpenEtlProcessesLatestEventForDataSourceType should
  //   return no events for that EtlContext.

  await dama_events.dispatch(final_event, eci_a, PG_ENV);

  cur_latest =
    await dama_events.queryOpenEtlProcessesLatestEventForDataSourceType(
      type,
      PG_ENV
    );

  expect(Array.isArray(cur_latest)).toBe(true);
  expect(cur_latest.length).toBe(1);
  expect(cur_latest[0]).toEqual(prev_latest[1]);

  prev_latest = cur_latest;

  await dama_events.dispatch(final_event, eci_b, PG_ENV);

  cur_latest =
    await dama_events.queryOpenEtlProcessesLatestEventForDataSourceType(
      type,
      PG_ENV
    );

  expect(Array.isArray(cur_latest)).toBe(true);
  expect(cur_latest.length).toBe(0);
});

test("queryNonOpenEtlProcessesLatestEventForDataSourceType", async () => {
  const source_name = uuid().slice(0, 10);
  const type = uuid();

  const {
    rows: [{ source_id }],
  } = await dama_db.query(
    {
      text: `
        INSERT INTO data_manager.sources ( name, type )
          VALUES ( $1, $2 )
          RETURNING source_id
      `,
      values: [source_name, type],
    },
    PG_ENV
  );

  const initial_event = {
    type: ":INITIAL",
  };

  const error_event = {
    type: ":ERROR",
    payload: { message: "Mock Error" },
  };

  const final_event = {
    type: ":FINAL",
  };

  const eci_a = await dama_events.spawnEtlContext(source_id, null, PG_ENV);
  const eci_b = await dama_events.spawnEtlContext(source_id, null, PG_ENV);

  let cur_latest: DamaEvent[];

  cur_latest =
    await dama_events.queryNonOpenEtlProcessesLatestEventForDataSourceType(
      type,
      PG_ENV
    );

  expect(Array.isArray(cur_latest)).toBe(true);
  expect(cur_latest.length).toBe(0);

  await dama_events.dispatch(initial_event, eci_a, PG_ENV);

  cur_latest =
    await dama_events.queryNonOpenEtlProcessesLatestEventForDataSourceType(
      type,
      PG_ENV
    );

  expect(cur_latest.length).toBe(0);

  await dama_events.dispatch(initial_event, eci_b, PG_ENV);

  cur_latest =
    await dama_events.queryNonOpenEtlProcessesLatestEventForDataSourceType(
      type,
      PG_ENV
    );

  expect(cur_latest.length).toBe(0);

  await dama_events.dispatch(error_event, eci_a, PG_ENV);

  cur_latest =
    await dama_events.queryNonOpenEtlProcessesLatestEventForDataSourceType(
      type,
      PG_ENV
    );

  expect(cur_latest.length).toBe(1);
  expect(cur_latest[0].etl_context_id).toBe(eci_a);
  expect(cur_latest[0].type).toBe(":ERROR");

  await dama_events.dispatch(error_event, eci_b, PG_ENV);

  cur_latest =
    await dama_events.queryNonOpenEtlProcessesLatestEventForDataSourceType(
      type,
      PG_ENV
    );

  expect(cur_latest.length).toBe(2);

  expect(cur_latest[0].etl_context_id).toBe(eci_a);
  expect(cur_latest[0].type).toBe(":ERROR");

  expect(cur_latest[1].etl_context_id).toBe(eci_b);
  expect(cur_latest[1].type).toBe(":ERROR");

  // Restarting EtlContexts in ERROR state will change their status from ERROR to OPEN.

  await dama_events.dispatch(
    {
      type: `:${uuid()}`,
    },
    eci_a,
    PG_ENV
  );

  cur_latest =
    await dama_events.queryNonOpenEtlProcessesLatestEventForDataSourceType(
      type,
      PG_ENV
    );

  expect(cur_latest.length).toBe(1);
  expect(cur_latest[0].etl_context_id).toBe(eci_b);
  expect(cur_latest[0].type).toBe(":ERROR");

  await dama_events.dispatch(
    {
      type: `:${uuid()}`,
    },
    eci_b,
    PG_ENV
  );

  cur_latest =
    await dama_events.queryNonOpenEtlProcessesLatestEventForDataSourceType(
      type,
      PG_ENV
    );

  expect(cur_latest.length).toBe(0);

  await dama_events.dispatch(final_event, eci_a, PG_ENV);

  cur_latest =
    await dama_events.queryNonOpenEtlProcessesLatestEventForDataSourceType(
      type,
      PG_ENV
    );

  expect(Array.isArray(cur_latest)).toBe(true);
  expect(cur_latest.length).toBe(1);
  expect(cur_latest[0].etl_context_id).toBe(eci_a);
  expect(cur_latest[0].type).toBe(":FINAL");

  await dama_events.dispatch(final_event, eci_b, PG_ENV);

  cur_latest =
    await dama_events.queryNonOpenEtlProcessesLatestEventForDataSourceType(
      type,
      PG_ENV
    );

  expect(cur_latest.length).toBe(2);
  expect(cur_latest[0].etl_context_id).toBe(eci_a);
  expect(cur_latest[0].type).toBe(":FINAL");
  expect(cur_latest[1].etl_context_id).toBe(eci_b);
  expect(cur_latest[1].type).toBe(":FINAL");
});
