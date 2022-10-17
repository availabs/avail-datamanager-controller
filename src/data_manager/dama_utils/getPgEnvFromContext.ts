import { Context } from "moleculer";

export default function getPgEnvFromCtx(ctx: Context) {
  const {
    // @ts-ignore
    meta: { pgEnv },
  } = ctx;

  if (!pgEnv) {
    throw new Error("ctx.meta.pgEnv is not set");
  }

  return pgEnv;
}
