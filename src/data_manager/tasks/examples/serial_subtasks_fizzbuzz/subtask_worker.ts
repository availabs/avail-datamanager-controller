import dama_events, { EtlEvent } from "../../../events";

export default async function main(initial_event: EtlEvent): Promise<EtlEvent> {
  const {
    payload: { n },
  } = initial_event;

  let type = n % 3 === 0 ? "FIZZ" : "";

  if (n % 5 === 0) {
    type = `${type}BUZZ`;
  }

  if (type) {
    dama_events.dispatch({
      type: `:${type}`,
    });
  }

  return {
    type: ":FINAL",
    payload: { n: n + 1 },
  };
}
