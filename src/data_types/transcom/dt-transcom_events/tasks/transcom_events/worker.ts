import { EtlContext, runInDamaContext } from "data_manager/contexts";

import main, { InitialEvent } from ".";

export default (etl_context: EtlContext) =>
  runInDamaContext(etl_context, () => main(etl_context.initial_event));
