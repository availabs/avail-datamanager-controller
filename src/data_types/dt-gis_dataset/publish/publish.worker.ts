import { runInDamaContext } from "data_manager/contexts";
import logger from "data_manager/logger";

import publish from "./publish";

export default async (etl_context: any) => {
  const { initial_event } = etl_context;

  const final_event = await runInDamaContext(etl_context, () =>
    publish(initial_event.payload)
  );

  return final_event;
};
