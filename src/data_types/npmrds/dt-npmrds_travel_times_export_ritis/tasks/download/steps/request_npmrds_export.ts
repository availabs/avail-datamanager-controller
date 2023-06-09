import { verifyIsInTaskEtlContext } from "data_manager/contexts";

import MassiveDataDownloader from "../../../puppeteer/MassiveDataDownloader";

import { NpmrdsState } from "data_types/npmrds/domain";

export default async function main(
  state: NpmrdsState,
  start_date: string,
  end_date: string,
  is_expanded = true
) {
  verifyIsInTaskEtlContext();

  const mdd = new MassiveDataDownloader();

  return mdd.requestNpmrdsDataExport(state, start_date, end_date, is_expanded);
}
