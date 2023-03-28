import { inspect } from "util";

import Moleculer from "moleculer";

import { runInDamaContext } from ".";

const dama_ctx_middleware: Moleculer.Middleware = {
  name: "Run REPL called actions in async_local_storage",

  localAction(next: any, action: any) {
    return function (ctx: Moleculer.Context) {
      const { meta: $repl } = ctx;

      if ($repl) {
        // @ts-ignore
        return runInDamaContext(ctx, () => next(ctx));
      }

      return next(ctx);
    };
  },
};

export default dama_ctx_middleware;
