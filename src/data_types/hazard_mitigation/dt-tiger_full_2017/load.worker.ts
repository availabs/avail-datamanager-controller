import { runInDamaContext } from "data_manager/contexts";

import publish from "./load";

export default async (etl_context: any) => {
  const { initial_event } = etl_context;

  const final_event = await runInDamaContext(etl_context, () =>
    publish(
      Object.assign({}, initial_event.payload, {
        etl_context_id: initial_event?.etl_context_id,
      })
    )
  );

  return final_event;
};
