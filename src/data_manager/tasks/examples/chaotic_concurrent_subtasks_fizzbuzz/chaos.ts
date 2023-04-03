import { getLoggerForContext } from "../../../logger";

const logger = getLoggerForContext();

export function injectChaos(chaos_factor: number) {
  if (Math.random() < chaos_factor) {
    if (Math.random() < 0.5) {
      logger.error("Chaos kill.");
      process.exit(789);
    }

    throw new Error("Wildcard!!!");
  }
}
