import _ from "lodash";
import dedent from "dedent";

import { TransactionContext } from "../index.d";

export default async function initializeDamaSourceMetadataUsingViews(
  txnCtx: TransactionContext
) {
  const {
    params: {
      // @ts-ignore
      newDamaView: { source_id: damaSourceId },
    },
  } = txnCtx;

  const damaSrcMetaSql = dedent(`
    SELECT
        metadata
      FROM data_manager.sources
      WHERE ( source_id = $1 )
  `);

  const { rows: damaSrcMetaResult } = await txnCtx.call("dama_db.query", {
    text: damaSrcMetaSql,
    values: [damaSourceId],
  });

  if (damaSrcMetaResult.length === 0) {
    throw new Error(`Invalid DaMa SourceID: ${damaSourceId}`);
  }

  const [{ metadata }] = damaSrcMetaResult;

  // Already has metadata. NoOp.
  if (metadata) {
    return metadata;
  }

  const damaViewMetadataSummarySql = dedent(`
    SELECT
        views_metadata_summary
      FROM _data_manager_admin.dama_source_distinct_view_metadata
      WHERE (
        ( source_id = $1 )
      )
  `);

  const { rows: damaViewMetadataSummaryResult } = await txnCtx.call(
    "dama_db.query",
    {
      text: damaViewMetadataSummarySql,
      values: [damaSourceId],
    }
  );

  const testQuery = `SELECT * FROM information_schema.tables WHERE table_name = '${txnCtx.params.newDamaView.table_name}'`;
  const { rows: testRows } = await txnCtx.call("dama_db.query", testQuery);

  console.log(
    JSON.stringify(
      {
        params: txnCtx.params,
        text: damaViewMetadataSummarySql,
        values: [damaSourceId],
        damaViewMetadataSummaryResult,
        testRows,
        testQuery,
      },
      null,
      4
    )
  );

  if (!damaViewMetadataSummaryResult.length) {
    throw new Error(`No DamaViews for DamaSource ${damaSourceId}`);
  }

  const [{ views_metadata_summary: damaViewMetadataSummary }] =
    damaViewMetadataSummaryResult;

  if (damaViewMetadataSummary.length > 1) {
    throw new Error(
      `DaMaSource ${damaSourceId} Views metadata are inconsistent. Cannot auto-initialize the source metadata.`
    );
  }

  const [
    {
      view_ids: [view_id],
      view_metadata,
    },
  ] = damaViewMetadataSummary;

  const initDataSrcMetadataSql = dedent(`
    CALL _data_manager_admin.initialize_dama_src_metadata_using_view( $1 );
  `);

  await txnCtx.call("dama_db.query", {
    text: initDataSrcMetadataSql,
    values: [view_id],
  });

  return view_metadata;
}
