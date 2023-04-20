import { readFile as readFileAsync } from "fs/promises";
import { join } from "path";

import dedent from "dedent";
import pgFormat from "pg-format";

import dama_db from "data_manager/dama_db";
import dama_events from "data_manager/events";
import logger from "data_manager/logger";

import getEtlContextLocalStateSqliteDb from "../../utils/getEtlContextLocalStateSqliteDb";
import {
  conflation_version,
  min_year,
  max_year,
} from "../../constants/conflation_map_meta";

export type InitialEvent = {
  type: ":INITIAL";
  payload: {
    etl_work_dir: string;
  };
  meta: { subtask_name: "generate_derivative_datasources" };
};

export type FinalEvent = {
  type: ":FINAL";
};

type PlaceholderValues = Record<string, string | number>;

// FIXME: Get these from somewhere so not hard-coded.
const SNAPPING_ALG_VERSION = "v0_0_2";
const KNN_K = 5;

const replacePlaceholdersWithValues = (
  sql_with_placeholders: string,
  placeholder_values: PlaceholderValues
) => {
  const placeholder_keys = Object.keys(placeholder_values);

  let sql = sql_with_placeholders;

  for (const k of placeholder_keys) {
    const v = placeholder_values[k];

    let re = new RegExp(`:${k}`, "g");

    sql = sql.replace(re, `${v}`);

    re = new RegExp(`:"${k}"`, "g");

    sql = sql.replace(re, `"${v}"`);

    re = new RegExp(`:'${k}'`, "g");

    sql = sql.replace(re, `'${v}'`);
  }

  return sql;
};

const execute_workflow_file = async (
  filename: string,
  placeholder_values: PlaceholderValues
) => {
  const filepath = join(__dirname, "./sql", filename);

  const sql_with_placeholders = await readFileAsync(filepath, {
    encoding: "utf8",
  });

  const sql = replacePlaceholdersWithValues(
    sql_with_placeholders,
    placeholder_values
  );

  logger.silly(sql);

  await dama_db.query(sql);
};

export default async function main(etl_work_dir: string) {
  const events = await dama_events.getAllEtlContextEvents();

  let final_event = events.find(({ type }) => type === ":FINAL");

  if (final_event) {
    return final_event;
  }

  const sqlite_db = getEtlContextLocalStateSqliteDb(etl_work_dir);

  const staging_schema = sqlite_db
    .prepare(
      `
        SELECT
            staging_schema
          FROM etl_context
      `
    )
    .pluck()
    .get();

  const placeholder_values = {
    staging_schema,
    min_year,
    max_year,
    conflation_version,
    snapping_alg_version: SNAPPING_ALG_VERSION,
    knn_k: KNN_K,
  };

  const initial_steps = [
    "create_transcom_events_gis_optimized_table.sql",
    "assign_events_to_admin_areas.sql",
    "create_transcom_events_onto_conflation_map.sql",
  ];

  for (const filename of initial_steps) {
    const done_type = `${filename}:DONE`;

    if (events.some(({ type }) => type === done_type)) {
      continue;
    }

    await execute_workflow_file(filename, placeholder_values);

    await dama_events.dispatch({
      type: done_type,
    });
  }

  const { rows } = await dama_db.query(
    dedent(
      pgFormat(
        `
          SELECT DISTINCT
              year
            FROM %I.transcom_events_gis_optimized
            ORDER BY year
        `,
        staging_schema
      )
    )
  );

  const event_years = rows.map(({ year }) => year);

  for (const event_year of event_years) {
    const filename = "snap_transcom_events_onto_conflation_map.sql";

    const done_type = `${filename}/${event_year}:DONE`;

    if (events.some(({ type }) => type === done_type)) {
      continue;
    }

    const snapping_placeholder_values = {
      ...placeholder_values,
      event_year,
    };

    await execute_workflow_file(filename, snapping_placeholder_values);

    await dama_events.dispatch({
      type: done_type,
    });
  }

  final_event = { type: ":FINAL" };

  await dama_events.dispatch(final_event);

  return final_event;
}
