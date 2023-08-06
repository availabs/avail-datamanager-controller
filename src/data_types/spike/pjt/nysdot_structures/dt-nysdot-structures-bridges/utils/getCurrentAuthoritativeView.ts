import dedent from "dedent";

import dama_db from "data_manager/dama_db";
import dama_meta from "data_manager/meta";
import { getPgEnv } from "data_manager/contexts";

type GetCurrentAuthoritativeViewFunction =
  | ((source_name: string, throw_if_not_exists: true) => Promise<number>)
  | ((
      source_name: string,
      throw_if_not_exists: false
    ) => Promise<number | null>);

const getCurrentAuthoritativeView: GetCurrentAuthoritativeViewFunction =
  // eslint-disable-next-line space-before-function-paren
  async function (
    source_name: string,
    throw_if_not_exists = false,
    pg_env = getPgEnv()
  ) {
    // Create the DamaSource if it does not exist.
    const { [source_name]: existing_dama_source } =
      await dama_meta.getDamaSourceMetadataByName([source_name], pg_env);

    const source_id = existing_dama_source?.source_id ?? null;

    if (source_id === null) {
      throw new Error(
        "The DamaSource has not been created for NYSDOT Bridges."
      );
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

    if (throw_if_not_exists) {
      if (authoritative_view_res.length < 1) {
        throw new Error(
          "There is no authoritative DamaView for NYSDOT Bridges."
        );
      }

      if (authoritative_view_res.length > 1) {
        throw new Error(
          "INVARIANT BROKEN: There is more than one authoritative DamaView for NYSDOT Bridges."
        );
      }
    }

    return authoritative_view_res?.[0]?.view_id ?? null;
  };

export default getCurrentAuthoritativeView;
