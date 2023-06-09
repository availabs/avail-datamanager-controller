import { runInDamaContext, TaskEtlContext } from "data_manager/contexts";

import main, { InitialEvent } from "./aggregate-etl";

type ThisTaskEtlContext = TaskEtlContext & { initial_event: InitialEvent };

export default async (etl_context: ThisTaskEtlContext) => {
  const final_event = await runInDamaContext(etl_context, () =>
    main(etl_context.initial_event)
  );

  return final_event;
};
