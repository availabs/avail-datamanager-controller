// WARNING: Code has not been run.

import main, { ThisTaskEtlContext } from "./publish.without-context";

export default async (etl_context: ThisTaskEtlContext) => {
  const final_event = await main(etl_context);

  return final_event;
};
