/*
 * NOTE: This module is used to initialize the automated ETL services.
 *
 *       It MUST ONLY be run in one instance, on one machine,
 *       otherwise multiple instances will be running the scheduled ETL concurrently.
 *
 *       USAGE: ln -s ./initialize_services.production.ts ./initialize_services.ts
 *
 *  ./moleculer.config.ts checks for the existence of ./initialize_services.ts
 *    and will call the default function.
 */

import { ServiceBroker } from "moleculer";

import { runInDamaContext } from "./src/data_manager/contexts";

async function initializeTranscom(broker: ServiceBroker) {
  await broker.waitForServices(["data_types/dt-transcom_events"]);

  await broker.call("data_types/dt-transcom_events.startTaskQueue");
  await broker.call("data_types/dt-transcom_events.scheduleTranscomEventsEtl", {
    cron: "11 1 * * *",
  });
  console.log("\n\nINITIALIZED TRANSCOM Services\n\n");
}

async function initializeNpmrds(broker: ServiceBroker) {
  await broker.waitForServices(["dama/data_types/npmrds"]);

  await broker.call("dama/data_types/npmrds.startTaskQueues");

  console.log("\n\nINITIALIZED NPMRDS Services\n\n");
}

export default async function initializeServices(
  broker: ServiceBroker
): Promise<void> {
  await runInDamaContext({ meta: { pgEnv: "npmrds" } }, async () => {
    await initializeTranscom(broker);
    await initializeNpmrds(broker);
  });
}
