import { createReadStream, writeFileSync } from "fs";
import { join, isAbsolute, basename } from "path";

import dedent from "dedent";
import _ from "lodash";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

import dama_db, { DamaDbQueryParamType } from "data_manager/dama_db";
import dama_events from "data_manager/events";
import dama_meta from "data_manager/meta";
import dama_gis from "data_manager/dama_gis";
import logger from "data_manager/logger";
import { runInDamaContext } from "data_manager/contexts";

import {
  DataSourceInitialMetadata,
  DamaViewInitialMetadata,
  DamaSourceName,
} from "data_manager/meta/domain";

import { getPgEnv, getEtlContextId } from "data_manager/contexts";

import dama_host_id from "constants/damaHostId";

import GeospatialDatasetIntegrator from "../../../../tasks/gis-data-integration/src/data_integrators/GeospatialDatasetIntegrator";

import { TableDescriptor } from "../../../../tasks/gis-data-integration/src/utils/GeospatialDataUtils";

export type { TableDescriptor } from "../../../../tasks/gis-data-integration/src/utils/GeospatialDataUtils";

type DamaViewInitialMetadataExtended = DamaViewInitialMetadata & {
  table_schema: string;
  table_name: string;
  etl_context_id: number;
};

export type ExtractID = string;
export type LayerName = string;

export type EtlConfig = {
  file_path: string;
  layer_name: string;
  source_info: DataSourceInitialMetadata;
  view_info: DamaViewInitialMetadataExtended;
  reviseTableDescriptor: (table_descriptor: TableDescriptor) => TableDescriptor;
  preserve_text_fields?: boolean;
};

enum ExtractDoneEventType {
  COPY_AND_INIT_DIR = "copyAndInitDir:DONE",
  METADATA = "getGeoDatasetMetadata:DONE",
  TBL_DSCRPTR = "getDefaultTableDescriptors:DONE",
  EXTRACT = "extract:DONE",
}

enum LoadDoneEventType {
  LOAD = "loadGisDatasetLayer:DONE",
}

export const pg_env_yargs_config = {
  alias: "p",
  describe: "The PostgresSQL Database",
  demandOption: true,
};

export const file_path_yargs_config = {
  alias: "f",
  describe: "The location of the GIS dataset.",
  demandOption: true,
};

export const logging_level_yargs_config = {
  alias: "l",
  describe: "The logging level",
  demandOption: false,
  default: "debug",
  choices: ["error", "warn", "info", "debug", "silly"],
};

export async function extract(file_path: string) {
  const file_stream = createReadStream(file_path);
  const file_name = basename(file_path);

  const gdi = new GeospatialDatasetIntegrator();

  return gdi.receiveDataset(<string>file_name, file_stream);
}

export async function getGeoDatasetMetadata(extract_id: ExtractID) {
  const gdi = new GeospatialDatasetIntegrator(extract_id);

  const metadata = gdi.getGeoDatasetMetadata();

  return metadata;
}

export async function getLayerNames(extract_id: ExtractID) {
  const gdi = new GeospatialDatasetIntegrator(extract_id);

  // @ts-ignore
  const { layerNameToId } = gdi;
  const layer_names = Object.keys(layerNameToId);

  return layer_names;
}

export async function updateLayerNamesToIds(
  extract_id: ExtractID,
  layer_name_to_id: Record<LayerName, number>
) {
  const gdi = new GeospatialDatasetIntegrator(extract_id);

  // @ts-ignore
  gdi.layerNameToId = layer_name_to_id;
}

export async function persistLayerTableDescriptor(
  extract_id: ExtractID,
  table_descriptor: TableDescriptor
) {
  const gdi = new GeospatialDatasetIntegrator(extract_id);

  // @ts-ignore
  await gdi.persistLayerTableDescriptor(table_descriptor);
}

export async function getTableDescriptor(
  extract_id: ExtractID,
  layer_name: string
) {
  const gdi = new GeospatialDatasetIntegrator(extract_id);

  const tableDescriptor = await gdi.getLayerTableDescriptor(layer_name);

  return tableDescriptor;
}

export async function revertAllTextFieldsInLayerTableDescriptor(
  extract_id: ExtractID,
  layer_name: string
) {
  const gdi = new GeospatialDatasetIntegrator(extract_id);

  const tableDescriptor = await gdi.revertAllTextFieldsInLayerTableDescriptor(
    layer_name
  );

  return tableDescriptor;
}

// Must be run within a DamaContext
export async function loadLayer(extract_id: ExtractID, layer_name: LayerName) {
  const gdi = new GeospatialDatasetIntegrator(extract_id);

  const { tableSchema: table_schema, tableName: table_name } =
    await gdi.loadTable({
      layerName: layer_name,
      pgEnv: getPgEnv(),
    });

  return { layer_name, table_schema, table_name };
}

// Must be run within a DamaContext
export async function performExtract(file_path: string) {
  const absolute_file_path = isAbsolute(file_path)
    ? file_path
    : join(process.cwd(), file_path);

  const events = await dama_events.getAllEtlContextEvents();

  let extract_done_event = events.find(
    ({ type }) => type === ExtractDoneEventType.EXTRACT
  );

  if (extract_done_event) {
    return extract_done_event.payload;
  }

  let copy_and_init_dir_event = events.find(
    ({ type }) => type === ExtractDoneEventType.COPY_AND_INIT_DIR
  );

  if (!copy_and_init_dir_event) {
    const extract_done_data = await extract(absolute_file_path);

    copy_and_init_dir_event = {
      type: ExtractDoneEventType.COPY_AND_INIT_DIR,
      payload: extract_done_data,
      meta: { dama_host_id },
    };

    await dama_events.dispatch(copy_and_init_dir_event);
  }

  const extract_done_data = copy_and_init_dir_event.payload;

  logger.debug(JSON.stringify({ extract_done_data }, null, 4));

  const { id: extract_id, workDirPath: workdir_path } = extract_done_data;

  const etl_context_id = getEtlContextId();
  const etl_context_id_fpath = join(workdir_path, "etl_context_id");
  writeFileSync(etl_context_id_fpath, `${etl_context_id}`);

  let got_metadata_event = events.find(
    ({ type }) => type === ExtractDoneEventType.METADATA
  );

  if (!got_metadata_event) {
    const metadata = await getGeoDatasetMetadata(extract_id!);

    got_metadata_event = {
      type: ExtractDoneEventType.METADATA,
      payload: metadata,
    };

    await dama_events.dispatch(got_metadata_event);
  }

  const geodataset_metadata = got_metadata_event.payload;

  const layer_names = await getLayerNames(extract_id!);

  let got_default_table_descriptors_event = events.find(
    ({ type }) => type === ExtractDoneEventType.TBL_DSCRPTR
  );

  if (!got_default_table_descriptors_event) {
    const table_descriptors = await Promise.all(
      layer_names.map((layer_name) =>
        getTableDescriptor(extract_id!, layer_name)
      )
    );

    got_default_table_descriptors_event = {
      type: ExtractDoneEventType.TBL_DSCRPTR,
      payload: table_descriptors,
    };

    await dama_events.dispatch(got_default_table_descriptors_event);
  }

  const { payload: table_descriptors } = got_default_table_descriptors_event;

  extract_done_event = {
    type: ExtractDoneEventType.EXTRACT,
    payload: {
      extract_done_data,
      geodataset_metadata,
      table_descriptors,
      extract_id,
      workdir_path,
      layer_names,
    },
  };

  await dama_events.dispatch(extract_done_event);

  return extract_done_event.payload;
}

// Must be run within a DamaContext
export async function reviseGisLayerTableDescriptors(
  table_descriptors: TableDescriptor | TableDescriptor[],
  extract_etl_context_id = getEtlContextId()
) {
  table_descriptors = Array.isArray(table_descriptors)
    ? table_descriptors
    : [table_descriptors];

  const extract_events = await dama_events.getAllEtlContextEvents(
    extract_etl_context_id
  );

  const copy_and_init_dir_event = extract_events.find(
    ({ type }) => type === ExtractDoneEventType.COPY_AND_INIT_DIR
  );

  if (!copy_and_init_dir_event) {
    throw new Error(
      `No copy_and_init_dir_event for etl_context ${extract_etl_context_id}.`
    );
  }

  const default_tbl_dscrptrs_event = extract_events.find(
    ({ type }) => type === ExtractDoneEventType.TBL_DSCRPTR
  );

  // Make sure the defaults were persisted in the database before overwriting them.
  if (!default_tbl_dscrptrs_event) {
    throw new Error(
      `No default table descriptors event for etl_context ${extract_etl_context_id}.`
    );
  }

  const {
    payload: { id: extract_id },
  } = copy_and_init_dir_event;

  for (const table_descriptor of table_descriptors) {
    await persistLayerTableDescriptor(extract_id, table_descriptor);

    dama_events.dispatch({
      type: "reviseGisLayerTableDescriptor:DONE",
      payload: { table_descriptor },
    });
  }
}

// NOTE: We do not check if already loaded so we can revise the TableDescriptor and reload.
// Must be run within a DamaContext
export async function loadGisDatasetLayer(
  layer_name: LayerName,
  extract_etl_context_id = getEtlContextId()
) {
  const extract_events = await dama_events.getAllEtlContextEvents(
    extract_etl_context_id
  );

  const extract_final_event = extract_events.find(
    ({ type }) => type === ExtractDoneEventType.EXTRACT
  );

  if (!extract_final_event) {
    throw new Error(
      `Could not find :FINAL event for extract etl_context_id ${extract_etl_context_id}`
    );
  }

  const {
    payload: { extract_id, layer_names },
  } = extract_final_event;

  if (!layer_names.includes(layer_name)) {
    throw new Error(
      `Layer name ${layer_name} does not exist in the GIS Dataset.`
    );
  }

  const load_start_event = {
    type: "loadGisDatasetLayer:START",
    payload: {
      layer_name,
    },
    meta: { extract_etl_context_id },
  };

  await dama_events.dispatch(load_start_event);

  const load_done_data = await loadLayer(extract_id, layer_name);

  const load_done_event = {
    type: LoadDoneEventType.LOAD,
    payload: load_done_data,
  };

  await dama_events.dispatch(load_done_event);

  return load_done_data;
}

export async function integrateIntoDama(
  layer_name: LayerName,
  data_source_initial_metadata: DataSourceInitialMetadata,
  // @ts-ignore FIXME
  data_view_initial_metadata: DamaViewInitialMetadataExtended = {},
  load_etl_context_id = getEtlContextId()
) {
  const load_events = await dama_events.getAllEtlContextEvents(
    load_etl_context_id
  );

  const layer_load_event = load_events
    .reverse()
    .find(
      (event) =>
        event.type === LoadDoneEventType.LOAD &&
        event.payload.layer_name === layer_name
    );

  if (!layer_load_event) {
    throw new Error(
      `Unable to find GIS Dataset layer load event for ${layer_name}.`
    );
  }

  console.log(JSON.stringify({ layer_load_event }, null, 4));

  const { name: source_name } = data_source_initial_metadata;

  // Create the DamaSource if it does not exist.

  const { [source_name]: existing_dama_source } =
    await dama_meta.getDamaSourceMetadataByName([source_name]);

  let source_id = existing_dama_source?.source_id ?? null;

  if (source_id === null) {
    const new_dama_source = await dama_meta.createNewDamaSource(
      data_source_initial_metadata
    );

    source_id = new_dama_source.source_id;
  }

  const [old_view_id] = await dama_meta.getCurrentActiveViewsForDamaSourceName(
    source_name
  );

  const {
    payload: { table_schema, table_name },
  } = layer_load_event;

  const start_event = {
    type: "integrateIntoDama:START",
    payload: { layer_name, source_id, old_view_id, table_schema, table_name },
  };

  await dama_events.dispatch(start_event);

  const new_view = {
    ...data_view_initial_metadata,
    table_schema,
    table_name,
    source_id,
  };

  const { view_id: new_view_id } = await dama_meta.createNewDamaView(new_view);

  const queries: DamaDbQueryParamType = ["BEGIN;"];

  if (old_view_id) {
    const make_cur_nonauth_sql = dedent(
      `
        UPDATE data_manager.views
          SET
            metadata = jsonb_set(
                         metadata,
                         ARRAY['dama', 'versionLinkedList', 'next'],
                         $1::TEXT::JSONB
                       ),
            active_end_timestamp = NOW()
          WHERE ( view_id = $2 )
      `
    );

    queries.push({
      text: make_cur_nonauth_sql,
      values: [new_view_id, old_view_id],
    });
  }

  const make_new_auth_sql = dedent(
    `
      UPDATE data_manager.views
        SET
          metadata = jsonb_set(
                       COALESCE(metadata, '{ "dama": {} }'::JSONB),
                       ARRAY['dama', 'versionLinkedList'],
                       $1::TEXT::JSONB
                     ),
          active_start_timestamp = NOW()
        WHERE ( view_id = $2 )
    `
  );

  queries.push({
    text: make_new_auth_sql,
    values: [{ previous: old_view_id, next: null }, new_view_id],
  });

  queries.push("COMMIT;");

  await dama_db.query(queries);

  const done_event = {
    type: "integrateIntoDama:DONE",
    payload: { view_id: new_view_id },
  };

  await dama_events.dispatch(done_event);

  return new_view_id;
}

export async function createMBTiles(dama_source_name: DamaSourceName) {
  const [view_id = null] =
    await dama_meta.getCurrentActiveViewsForDamaSourceName(dama_source_name);

  if (view_id === null) {
    throw new Error(
      `INVARIANT BROKEN: No authoritative DamaView found for ${dama_source_name}.`
    );
  }

  const start_event = {
    type: "create_mbtiles:START",
    meta: { dama_host_id, view_id },
  };

  await dama_events.dispatch(start_event);

  const done_data = await dama_gis.createGisDatasetViewMbtiles(view_id);

  const mbtiles_meta = _.pick(done_data, [
    "tileset_name",
    "source_id",
    "source_layer_name",
    "source_type",
  ]);

  const update_view_metadata_sql = dedent(`
    UPDATE data_manager.views
      SET metadata = jsonb_set (
                       metadata,
                       ARRAY['dama', 'mbtiles'],
                       $1::TEXT::JSONB
                     )
      WHERE ( view_id = $2 )
  `);

  await dama_db.query({
    text: update_view_metadata_sql,
    values: [mbtiles_meta, view_id],
  });

  const final_event = {
    type: "create_mbtiles:DONE",
    payload: done_data,
  };

  await dama_events.dispatch(final_event);

  console.log(`MBTiles created.\n${JSON.stringify(mbtiles_meta, null, 4)}`);
}

export default async function etl(config: EtlConfig) {
  const {
    file_path,
    layer_name,
    source_info,
    view_info,
    preserve_text_fields = false,
  } = config;

  try {
    const initial_event = {
      type: `${view_info.table_schema}/${view_info.table_name}:INITIAL`,
      meta: { dama_host_id },
    };

    await dama_events.dispatch(initial_event);

    const { extract_id, table_descriptors: default_table_descriptors } =
      await performExtract(file_path);

    const table_descriptor = default_table_descriptors.find(
      ({ layerName }) => layerName === layer_name
    );

    if (!table_descriptor) {
      throw new Error(
        `INVARIANT BROKEN: The ${source_info.name} layer name is expected to be ${layer_name}.`
      );
    }

    let revised_table_descriptor = table_descriptor;

    if (preserve_text_fields) {
      revised_table_descriptor =
        await revertAllTextFieldsInLayerTableDescriptor(extract_id, layer_name);
    }

    revised_table_descriptor = {
      ..._.cloneDeep(revised_table_descriptor),
      tableSchema: view_info.table_schema,
      tableName: view_info.table_name,
    };

    if (config.reviseTableDescriptor) {
      revised_table_descriptor = config.reviseTableDescriptor(
        revised_table_descriptor
      );
    }

    await reviseGisLayerTableDescriptors(revised_table_descriptor);

    await loadGisDatasetLayer(layer_name);

    const integrate_done_data = await integrateIntoDama(
      layer_name,
      source_info,
      {
        ...view_info,
        etl_context_id: getEtlContextId() as number,
      }
    );

    await createMBTiles(source_info.name);

    const final_event = {
      type: `${view_info.table_schema}/${view_info.table_name}/etl:FINAL`,
      payload: integrate_done_data,
    };

    await dama_events.dispatch(final_event);
  } catch (err) {
    const { message, stack } = err as Error;

    const error_event = {
      type: `${view_info.table_schema}/${view_info.table_name}/etl:ERROR`,
      payload: { message, stack },
      error: true,
    };

    dama_events.dispatch(error_event);

    throw err;
  }
}

export async function runETLFromCLI(
  layer_config: Omit<EtlConfig, "file_path">
) {
  // @ts-ignore
  const { pg_env, file_path, logging_level } = yargs(hideBin(process.argv))
    .strict()
    .options({
      file_path: file_path_yargs_config,
      pg_env: pg_env_yargs_config,
      logging_level: logging_level_yargs_config,
    }).argv;

  logger.level = logging_level;

  const etl_config = { ...layer_config, file_path };

  const etl_context_id = await dama_events.spawnEtlContext(null, null, pg_env);

  logger.info(`==> etl_context_id: ${etl_context_id}`);

  await runInDamaContext(
    {
      meta: { pgEnv: pg_env, etl_context_id },
    },
    () => etl(etl_config)
  );
}
