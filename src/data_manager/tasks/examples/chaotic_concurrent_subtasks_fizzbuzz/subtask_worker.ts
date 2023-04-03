import dedent from "dedent";

import { FSA } from "flux-standard-action";

import dama_db from "../../../dama_db";
import dama_events from "../../../events";
import { getEtlContextId } from "../../../contexts";
import { getLoggerForContext, LoggingLevel } from "../../../logger";

import { injectChaos } from "./chaos";

const logger = getLoggerForContext();
logger.level = process.env.AVAIL_LOGGING_LEVEL || LoggingLevel.debug;

export default async function main(initial_event: FSA): Promise<FSA> {
  const {
    // @ts-ignore
    payload: { n, chaos_factor },
  } = initial_event;

  const heartbeat_interval = setInterval(
    () => logger.debug(`HEARTBEAT: ${new Date().toISOString()}`),
    2000
  );

  injectChaos(chaos_factor);

  let type = n % 3 === 0 ? "FIZZ" : "";

  if (n % 5 === 0) {
    type = `${type}BUZZ`;
  }

  if (type) {
    const etl_context_id = getEtlContextId();

    const idempotency_sql = dedent(`
      SELECT
          ( initial_event_id = latest_event_id ) AS only_initial_event
        FROM data_manager.etl_contexts
        WHERE ( etl_context_id = $1 )
    `);

    const {
      rows: [{ only_initial_event }],
    } = await dama_db.query({
      text: idempotency_sql,
      values: [etl_context_id],
    });

    injectChaos(chaos_factor);

    if (only_initial_event) {
      dama_events.dispatch({
        type: `:${type}`,
      });
    }

    injectChaos(chaos_factor);
  }

  injectChaos(chaos_factor);

  clearInterval(heartbeat_interval);

  return {
    type: ":FINAL",
    // @ts-ignore
    payload: { n: n + 1 },
  };
}
