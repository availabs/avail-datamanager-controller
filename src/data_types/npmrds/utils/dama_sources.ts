import dama_meta from "data_manager/meta";
import logger from "data_manager/logger";

import {
  toposortedSourceNames,
  npmrdsDataSourcesInitialMetadataByName,
  toposortedNpmrdsDataSourcesInitialMetadata,
} from "../domain";
import {
  DamaSource,
  DataSourceInitialMetadata,
} from "data_manager/meta/domain";

export async function getToposortedDamaSourcesMeta() {
  const metaByName: Record<string, object> =
    await dama_meta.getDamaSourceMetadataByName(toposortedSourceNames);

  const toposortedMeta = <Array<DataSourceInitialMetadata | DamaSource>>(
    toposortedSourceNames.map(
      (name) =>
        // NOTE: if not in metaByName, will not have a source_id
        metaByName[name] || npmrdsDataSourcesInitialMetadataByName[name]
    )
  );

  logger.silly(
    `==> npmrds getToposortedDamaSourcesMeta ${JSON.stringify(
      toposortedMeta,
      null,
      4
    )}`
  );

  return toposortedMeta;
}

export async function initializeDamaSources() {
  const existingToposortedDamaSrcMeta = await getToposortedDamaSourcesMeta();

  if (
    // @ts-ignore
    existingToposortedDamaSrcMeta.every(({ source_id: id }) =>
      Number.isFinite(id)
    )
  ) {
    logger.info(
      "==> npmrds initializeDamaSources: All NPMRDS DataSources already created."
    );

    return <DamaSource[]>existingToposortedDamaSrcMeta;
  }

  const toposortedDamaSrcMeta = <DamaSource[]>(
    await dama_meta.loadToposortedDamaSourceMetadata(
      toposortedNpmrdsDataSourcesInitialMetadata
    )
  );

  logger.info("==> npmrds initializeDamaSources: NPMRDS DataSources created.");

  logger.debug(
    `==> npmrds initializeDamaSources ${JSON.stringify(
      { toposortedDamaSrcMeta },
      null,
      4
    )}`
  );

  return toposortedDamaSrcMeta;
}
