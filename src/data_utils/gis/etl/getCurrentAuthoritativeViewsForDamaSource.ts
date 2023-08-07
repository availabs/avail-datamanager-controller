import dedent from "dedent";

import dama_db from "data_manager/dama_db";
import dama_meta from "data_manager/meta";
import { getPgEnv } from "data_manager/contexts";

// Assumption: active_end_timestamp is NULL for authoritative views.
export default async function getCurrentAuthoritativeViewsForDamaSource(
  source_name: string,
  pg_env = getPgEnv()
) {
  // Create the DamaSource if it does not exist.
  const { [source_name]: existing_dama_source } =
    await dama_meta.getDamaSourceMetadataByName([source_name], pg_env);

  const source_id = existing_dama_source?.source_id ?? null;

  if (source_id === null) {
    throw new Error(`The DamaSource has not been created for ${source_name}.`);
  }

  // Get the current authoritative
  const current_authoritative_view_id_sql = dedent(`
    SELECT
        view_id
      FROM data_manager.sources AS a
        INNER JOIN data_manager.views AS b
          USING ( source_id )
      WHERE (
        ( source_id = $1 )
        AND
        ( active_end_timestamp IS NULL )
      )
  `);

  const { rows: authoritative_view_res } = await dama_db.query(
    {
      text: current_authoritative_view_id_sql,
      values: [source_id],
    },
    pg_env
  );

  const view_ids = authoritative_view_res.map(({ view_id }) => view_id);

  return view_ids;
}
