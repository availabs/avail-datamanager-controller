import dedent from "dedent";
import pgFormat from "pg-format";

import dama_db from "data_manager/dama_db";
import dama_meta from "data_manager/meta";
import logger from "data_manager/logger";
import { getEtlContextId } from "data_manager/contexts";

export async function createView(
  view_values: Record<string, string | number | any>
) {
  const {
    source_id,
    user_id,
    customViewAttributes,
    viewMetadata,
    viewDependency,
  } = view_values;

  let newDamaView = await dama_meta.createNewDamaView({
    user_id,
    source_id,
    etl_context_id: getEtlContextId(),
    view_dependencies: [viewDependency],
    metadata: { ...(customViewAttributes || {}), ...(viewMetadata || {}) },
  });

  const {
    view_id: damaViewId,
    table_schema: origTableSchema,
    table_name: origTableName,
    metadata,
  } = newDamaView;

  const table_schema = origTableSchema || "gis_datasets";
  let table_name = origTableName;

  // Assign the default table_name if one wasn't specified
  if (!origTableName) {
    const text =
      "SELECT _data_manager_admin.dama_view_name($1) AS dama_view_name;";

    const {
      rows: [{ dama_view_name }],
    } = await dama_db.query({
      text,
      values: [damaViewId],
    });

    table_name = dama_view_name;
  }

  if (origTableSchema !== table_schema || origTableName !== table_name) {
    const updateViewMetaSql = dedent(
      `
          UPDATE data_manager.views
            SET
              table_schema  = $1,
              table_name    = $2,
              data_table    = $3
            WHERE ( view_id = $4 )
        `
    );

    const dataTable = pgFormat("%I.%I", table_schema, table_name);

    const q = {
      text: updateViewMetaSql,
      values: [table_schema, table_name, dataTable, damaViewId],
    };

    await dama_db.query(q);

    const { rows } = await dama_db.query({
      text: "SELECT * FROM data_manager.views WHERE ( view_id = $1 );",
      values: [damaViewId],
    });

    newDamaView = rows[0];
  }

  return newDamaView;
}

export async function createSource(
  source_values: Record<string, string | number>
  ) {
  logger.info(`Create Source Called  :  ${source_values}`);
  // const uniqId = uuid().replace(/[^0-9A-Z]/gi, "");
  let damaSource: any;
  const {
    name, // = `untitled dataset ${uniqId}`,
    type = "gis_dataset",
    update_interval = "",
    description = "",
  } = source_values;
  // create source
  try {
    logger.info(`In the create source method: \nName: ${name}, \ntype: ${type}, \nupdate_interval: ${update_interval}, \ndescription: ${description},`);
    damaSource = await dama_meta.createNewDamaSource({
      name,
      type,
      update_interval,
      description,
    });
  } catch (err) {
    logger.error(`createNewDamaSource error:, ${JSON.stringify(err, null, 3)}`);
  }
  return damaSource
}
