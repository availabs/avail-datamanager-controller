import _ from "lodash";
import dedent from "dedent";
import pgFormat from "pg-format";

import { Context as MoleculerContext } from "moleculer";

import { FSA } from "flux-standard-action";

import EventTypes from "../../../constants/EventTypes";

import { DamaView } from "../index.d";

export default async function createNewDamaView(ctx: MoleculerContext) {
  const { params } = ctx;
  const { newDamaSource } = params;

  const { eventsByType } = params;

  const [createDamaViewEvent] = <[FSA]>(
    eventsByType[EventTypes.QUEUE_CREATE_NEW_DAMA_VIEW]
  );

  const queuedDamaViewMeta: any = _.cloneDeep(createDamaViewEvent.payload);

  if (queuedDamaViewMeta.source_id) {
    if (newDamaSource) {
      throw new Error(
        "If an ETL Context creates both a DamaSource and a DamaView, the DamaView cannot specify a source_id."
      );
    }
  } else {
    if (!newDamaSource) {
      throw new Error("No source_id is not provided for the new DamaView.");
    }
    // @ts-ignore
    queuedDamaViewMeta.source_id = newDamaSource.source_id;
  }

  if (queuedDamaViewMeta.table_schema && queuedDamaViewMeta.table_name) {
    queuedDamaViewMeta.data_table = pgFormat(
      "%I.%I",
      queuedDamaViewMeta.table_schema,
      queuedDamaViewMeta.table_name
    );
  }

  let newDamaView = <DamaView>(
    await ctx.call("dama/metadata.createNewDamaView", queuedDamaViewMeta)
  );

  const {
    view_id: damaViewId,
    table_schema: origTableSchema,
    table_name: origTableName,
  } = newDamaView;

  const table_schema = origTableSchema || "gis_datasets";
  let table_name = origTableName;

  // Assign the default table_name if one wasn't specified
  if (!origTableName) {
    const text =
      "SELECT _data_manager_admin.dama_view_name($1) AS dama_view_name;";

    const {
      rows: [{ dama_view_name }],
    } = await ctx.call("dama_db.query", {
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

    const sql = {
      text: updateViewMetaSql,
      values: [table_schema, table_name, dataTable, damaViewId],
    };

    await ctx.call("dama_db.query", sql);

    const newDamaViewSql = dedent(`
      SELECT
          *
        FROM data_manager.views
        WHERE ( view_id = $1 )
      ;
    `);

    const { rows } = await ctx.call("dama_db.query", {
      text: newDamaViewSql,
      values: [damaViewId],
    });

    newDamaView = rows[0];
  }

  params.newDamaView = newDamaView;
}
