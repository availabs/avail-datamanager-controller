import default_logger from "data_manager/logger";

export function injectChaos(chaos_factor: number, logger = default_logger) {
  if (Math.random() < chaos_factor) {
    logger.error("Chaos kill.");

    if (Math.random() < 0.5) {
      process.exit(789);
    }

    throw new Error("Wildcard!!!");
  }
}
