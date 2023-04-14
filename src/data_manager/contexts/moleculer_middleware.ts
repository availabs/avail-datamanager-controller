import Moleculer from "moleculer";
import { NextFunction } from "express";

import { runInDamaContext } from ".";

export default {
  // @ts-ignore
  localAction(next: NextFunction) {
    return function (ctx: Moleculer.Context) {
      const { meta } = ctx;

      // @ts-ignore
      if (meta?.pgEnv) {
        try {
          // @ts-ignore
          return runInDamaContext({ meta }, () => {
            return next(ctx);
          });
        } catch (err) {
          console.error(err);
          throw err;
        }
      }

      return next(ctx);
    };
  },
};
