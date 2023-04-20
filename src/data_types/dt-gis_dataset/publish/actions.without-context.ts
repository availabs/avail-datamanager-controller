// WARNING: Code has not been run.

import { inspect } from "util";

import dedent from "dedent";
import pgFormat from "pg-format";

import dama_db from "data_manager/dama_db";
import dama_meta from "data_manager/meta";
import logger from "data_manager/logger";

import { TaskEtlContext } from "data_manager/contexts";

export async function createView(
  etl_context: TaskEtlContext,
  view_values: any
) {
  const {
    meta: { pgEnv: pg_env, etl_context_id },
  } = etl_context;

  const { source_id, user_id, customViewAttributes } = view_values;

  let newDamaView = await dama_meta.createNewDamaView(
    {
      source_id,
      user_id,
      etl_context_id,
      metadata: { ...customViewAttributes },
    },
    pg_env
  );

  const {
    view_id: damaViewId,
    table_schema: origTableSchema,
    table_name: origTableName,
  } = newDamaView;

  logger.debug(`new dama view ${inspect(newDamaView)}`);

  const table_schema = origTableSchema || "gis_datasets";
  let table_name = origTableName;

  // Assign the default table_name if one wasn't specified
  if (!origTableName) {
    const text =
      "SELECT _data_manager_admin.dama_view_name($1) AS dama_view_name;";

    const {
      rows: [{ dama_view_name }],
    } = await dama_db.query(
      {
        text,
        values: [damaViewId],
      },
      pg_env
    );

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

    await dama_db.query(q, pg_env);

    const { rows } = await dama_db.query(
      {
        text: "SELECT * FROM data_manager.views WHERE ( view_id = $1 );",
        values: [damaViewId],
      },
      pg_env
    );

    newDamaView = rows[0];
  }

  return newDamaView;
}

export async function createSource(
  etl_context: TaskEtlContext,
  source_values: any
) {
  const {
    meta: { pgEnv: pg_env },
  } = etl_context;

  const {
    name, // = `untitled dataset ${uniqId}`,
    type = "gis_dataset",
    update_interval = "",
    description = "",
  } = source_values;
  // create source

  logger.debug(
    `values for create source: name=${name} type=${type} update_interval=${update_interval} description=${description}`
  );

  const damaSource = await dama_meta.createNewDamaSource(
    {
      name,
      type,
      update_interval,
      description,
    },
    pg_env
  );

  return damaSource;
}
