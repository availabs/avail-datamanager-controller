import { FSA } from "flux-standard-action";
import dama_events from "../../../events";

export default async function main(initial_event: FSA): Promise<FSA> {
  const {
    // @ts-ignore
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
    // @ts-ignore
    payload: { n: n + 1 },
  };
}
