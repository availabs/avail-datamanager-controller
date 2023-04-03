import { getLoggerForContext } from "../../../logger";

const logger = getLoggerForContext();

export function injectChaos(chaos_factor: number) {
  if (Math.random() < chaos_factor) {
    logger.error("Chaos kill.");

    if (Math.random() < 0.5) {
      process.exit(789);
    }

    throw new Error("Wildcard!!!");
  }
}
