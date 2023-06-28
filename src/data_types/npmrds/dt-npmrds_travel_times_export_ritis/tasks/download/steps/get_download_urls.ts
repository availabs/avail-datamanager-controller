import { verifyIsInTaskEtlContext } from "data_manager/contexts";
import dama_events from "data_manager/events";

import MyHistory from "../../../puppeteer/MyHistory";
import { TaskDoneEventType as NpmrdsExportRequestedEventType } from "../../../puppeteer/MassiveDataDownloader";

export default async function main() {
  verifyIsInTaskEtlContext();

  const events = await dama_events.getAllEtlContextEvents();

  const export_requested_event = events.find(
    ({ type }) => type === NpmrdsExportRequestedEventType
  );

  if (!export_requested_event) {
    throw new Error(
      `Did not find a ${NpmrdsExportRequestedEventType} event in the ETL Context's events`
    );
  }

  const my_history = new MyHistory();

  const download_links = await my_history.waitForExportRequestReadyToDownload(
    export_requested_event.payload
  );

  return download_links;
}
