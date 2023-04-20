// WARNING: Code has not been run.

import { runInDamaContext, TaskEtlContext } from "data_manager/contexts";

import main, { InitialEvent } from "./publish.with-context";

type ThisTaskEtlContext = TaskEtlContext & { initial_event: InitialEvent };

export default async (etl_context: ThisTaskEtlContext) => {
  const { initial_event } = etl_context;

  const final_event = await runInDamaContext(etl_context, () =>
    main(initial_event)
  );

  return final_event;
};
