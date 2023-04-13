// https://www.lucasamos.dev/articles/mocknodefetch

jest.mock("node-fetch");
jest.mock("../utils/TranscomAuthTokenCollector");

import fetch from "node-fetch";
import tmp from "tmp";
import { DateTime } from "luxon";
import { v4 as uuid } from "uuid";

import etl_dir from "constants/etlDir";
import dama_events from "data_manager/events";
import { runInDamaContext } from "data_manager/contexts";
import { getLoggerForContext, LoggingLevel } from "data_manager/logger";

import getEtlContextLocalStateSqliteDb from "../../utils/getEtlContextLocalStateSqliteDb";
import TranscomAuthTokenCollector from "../../utils/TranscomAuthTokenCollector";

import getEtlWorkDir from "../../utils/etlWorkDir";

import collectTranscomEventIdsForTimeRange, {
  url as historical_events_url,
} from ".";

const PG_ENV = "ephemeral_test_db";

const mock_jwt = uuid();

const parsed_historical_events_url = new URL(historical_events_url);

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

test("collects event_ids from seen_event and requests them", async () => {
  // We use a tmp dir so there are no name collisions.
  // Recall the ephemeral_test_db database is DROPPED before tests run.
  // That means the same etl_context_ids will reappear.
  const { name: tmp_dir, removeCallback } = tmp.dirSync({
    prefix: "dt-transcom_events.test.",
    tmpdir: etl_dir,
    unsafeCleanup: true,
  });

  try {
    const etl_context_id = await dama_events.spawnEtlContext(
      null,
      null,
      PG_ENV
    );

    const etl_work_dir = getEtlWorkDir(PG_ENV, etl_context_id, tmp_dir);
    const event_ids = ["ORI249285209", "ORI1237164741", "ORI730824"].sort();

    // @ts-ignore
    fetch.mockImplementation((url: string, options: any) => {
      // console.log(JSON.stringify({ url, options }, null, 4));

      const parsed_url = new URL(url);

      const { headers } = options;

      expect(parsed_url.origin).toBe(parsed_historical_events_url.origin);
      expect(parsed_url.pathname).toBe(parsed_historical_events_url.pathname);

      expect(headers["Content-Type"]).toBe("application/json");
      expect(headers.authenticationtoken).toBe(`Bearer ${mock_jwt}`);

      return Promise.resolve({
        json: () => Promise.resolve({ data: event_ids.map((id) => ({ id })) }),
      });
    });

    const sqlite_db = getEtlContextLocalStateSqliteDb(etl_work_dir);

    const start_date_time = DateTime.now().startOf("day").plus({ hours: 12 });

    const end_date_time = start_date_time.plus({ minutes: 30 });

    const initial_event = {
      type: ":INITIAL",
      payload: {
        etl_work_dir,
        start_timestamp: start_date_time.toISO(),
        end_timestamp: end_date_time.toISO(),
      },
    };

    await dama_events.dispatch(initial_event, etl_context_id, PG_ENV);

    await runInDamaContext(
      {
        initial_event,
        meta: { pgEnv: PG_ENV, etl_context_id },
      },
      () => {
        const logger = getLoggerForContext();

        logger.transports.forEach((transport) => {
          transport.level = LoggingLevel.error;
        });

        return collectTranscomEventIdsForTimeRange(
          etl_work_dir,
          initial_event.payload.start_timestamp,
          initial_event.payload.end_timestamp
        );
      }
    );

    const seen_event_ids = sqlite_db
      .prepare(
        `
          SELECT
              event_id
            FROM seen_event
            ORDER BY 1
        `
      )
      .pluck()
      .all();

    expect(seen_event_ids).toEqual(event_ids);
  } finally {
    removeCallback();
  }
});
