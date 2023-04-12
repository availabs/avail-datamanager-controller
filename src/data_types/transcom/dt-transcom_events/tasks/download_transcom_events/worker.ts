import { runInDamaContext, TaskEtlContext } from "data_manager/contexts";

import main, { InitialEvent } from ".";

type ThisTaskEtlContext = TaskEtlContext & { initial_event: InitialEvent };

export default async (etl_context: ThisTaskEtlContext) => {
  const {
    initial_event: {
      payload: { etl_work_dir },
    },
  } = etl_context;

  const final_event = await runInDamaContext(etl_context, () =>
    main(etl_work_dir)
  );

  return final_event;
};
