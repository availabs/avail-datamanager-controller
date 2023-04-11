// https://www.lucasamos.dev/articles/mocknodefetch

jest.mock("node-fetch");
jest.mock("../utils/TranscomAuthTokenCollector");

import fetch from "node-fetch";
import tmp from "tmp";

import etl_dir from "constants/etlDir";
import dama_events from "data_manager/events";
import { runInDamaContext } from "data_manager/contexts";

import getEtlContextLocalStateSqliteDb from "../utils/getEtlContextLocalStateSqliteDb";
import TranscomAuthTokenCollector from "../utils/TranscomAuthTokenCollector";

import downloadTranscomEventsExpanded from ".";

const PG_ENV = "ephemeral_test_db";

const mock_jwt = "foobarbaz";

beforeAll(() => {
  // @ts-ignore
  TranscomAuthTokenCollector.mockImplementation(() => {
    return {
      getJWT: () => Promise.resolve(mock_jwt),

      // eslint-disable-next-line @typescript-eslint/no-empty-function
      close: () => {},
    };
  });
});

test("reads event_ids from seen_event and requests them", async () => {
  const { name: etl_work_dir, removeCallback } = tmp.dirSync({
    prefix: "dt-transcom_events.test.",
    tmpdir: etl_dir,
    unsafeCleanup: true,
  });

  try {
    console.log("==> etl_work_dir:", etl_work_dir);

    const event_ids = ["ORI249285209", "ORI1237164741", "ORI730824"].sort();

    // @ts-ignore
    fetch.mockImplementationOnce((url: string, options: any) => {
      const parsed_url = new URL(url);

      console.log(JSON.stringify({ parsed_url }, null, 4));

      const requested_ids = [...parsed_url.searchParams.entries()]
        .map(([k, v]) => (k === "id" ? v : null))
        .filter(Boolean)
        .sort();

      const { headers } = options;

      expect(requested_ids).toEqual(event_ids);
      expect(headers["Content-Type"]).toBe("application/json");
      expect(headers.authenticationtoken).toBe(`Bearer ${mock_jwt}`);

      return Promise.resolve({
        json: () => Promise.resolve({ data: event_ids.map((ID) => ({ ID })) }),
      });
    });

    const sqlite_db = getEtlContextLocalStateSqliteDb(etl_work_dir);

    const insert_stmt = sqlite_db.prepare(`
      INSERT INTO seen_event ( event_id )
        VALUES ( ? )
      ;
    `);

    for (const event_id of event_ids) {
      insert_stmt.run(event_id);
    }

    const etl_context_id = await dama_events.spawnEtlContext(
      null,
      null,
      PG_ENV
    );

    const initial_event = { type: ":INITIAL" };

    await dama_events.dispatch(initial_event, etl_context_id, PG_ENV);

    const final_event = await runInDamaContext(
      {
        initial_event,
        meta: { pgEnv: PG_ENV, etl_context_id },
      },
      () => downloadTranscomEventsExpanded(etl_work_dir, event_ids.length, 0)
    );

    expect(final_event).toBeTruthy();
  } finally {
    removeCallback();
  }
});

test("skips already downloaded event_ids", async () => {
  const { name: etl_work_dir, removeCallback } = tmp.dirSync({
    prefix: "dt-transcom_events.test.",
    tmpdir: etl_dir,
    unsafeCleanup: true,
  });

  try {
    console.log("==> etl_work_dir:", etl_work_dir);

    const event_ids = ["ORI249285209", "ORI1237164741", "ORI730824"].sort();

    const [already_downloaded_event_id] = event_ids;
    const awaiting_download_event_ids = event_ids.slice(1);

    // @ts-ignore
    fetch.mockImplementationOnce((url: string, options: any) => {
      const parsed_url = new URL(url);

      const requested_ids = [...parsed_url.searchParams.entries()]
        .map(([k, v]) => (k === "id" ? v : null))
        .filter(Boolean)
        .sort();

      const { headers } = options;

      expect(requested_ids).toEqual(awaiting_download_event_ids);
      expect(headers["Content-Type"]).toBe("application/json");
      expect(headers.authenticationtoken).toBe(`Bearer ${mock_jwt}`);

      return Promise.resolve({
        json: () => Promise.resolve({ data: event_ids.map((ID) => ({ ID })) }),
      });
    });

    const sqlite_db = getEtlContextLocalStateSqliteDb(etl_work_dir);

    const insert_stmt = sqlite_db.prepare(`
      INSERT INTO seen_event ( event_id )
        VALUES ( ? )
      ;
    `);

    for (const event_id of event_ids) {
      insert_stmt.run(event_id);
    }

    sqlite_db
      .prepare(
        `
          INSERT INTO downloaded_event ( event_id, file_path )
            VALUES ( ?, ? )
          ;
        `
      )
      .run(already_downloaded_event_id, "mock");

    const etl_context_id = await dama_events.spawnEtlContext(
      null,
      null,
      PG_ENV
    );

    const initial_event = { type: ":INITIAL" };

    await dama_events.dispatch(initial_event, etl_context_id, PG_ENV);

    const final_event = await runInDamaContext(
      {
        initial_event,
        meta: { pgEnv: PG_ENV, etl_context_id },
      },
      () => downloadTranscomEventsExpanded(etl_work_dir, event_ids.length, 0)
    );

    expect(final_event).toBeTruthy();
  } finally {
    removeCallback();
  }
});
