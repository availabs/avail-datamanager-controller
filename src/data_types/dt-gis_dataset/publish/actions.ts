import _ from "lodash";
import dedent from "dedent";
import pgFormat from "pg-format";




export async function createView(ctx, view_values) {
 
  const {
    source_id,
    user_id
  } = view_values

  // if (queuedDamaViewMeta.table_schema && queuedDamaViewMeta.table_name) {
  //   queuedDamaViewMeta.data_table = pgFormat(
  //     "%I.%I",
  //     queuedDamaViewMeta.table_schema,
  //     queuedDamaViewMeta.table_name
  //   );
  // }

  let newDamaView = (
    await ctx.call("dama/metadata.createNewDamaView", {source_id, user_id})
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

    const q = {
      text: updateViewMetaSql,
      values: [table_schema, table_name, dataTable, damaViewId],
    };

    await ctx.call("dama_db.query", q);

    const { rows } = await ctx.call("dama_db.query", {
      text: "SELECT * FROM data_manager.views WHERE ( view_id = $1 );",
      values: [damaViewId],
    });

    newDamaView = rows[0];
  }

  return newDamaView;
}

export async function createSource(ctx, source_values) {
  // const uniqId = uuid().replace(/[^0-9A-Z]/gi, "");
  const {
      name, // = `untitled dataset ${uniqId}`,
      type = 'gis_dataset',
      update_interval =  '',
      description = '',
      statistics = {},
      metadata = {},
  } = source_values
  // create source
  let damaSource = await ctx.call(
      "dama/metadata.createNewDamaSource",
      {
        name,
        type,
        update_interval,
        description,
        statistics,
        metadata
      }
  )
  return damaSource
}
