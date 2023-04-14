import dedent from "dedent";
import pgFormat from "pg-format";

import dama_db from "data_manager/dama_db";
import logger from "data_manager/logger";
import { verifyIsInTaskEtlContext } from "data_manager/contexts";

import getEtlContextLocalStateSqliteDb from "../../../../utils/getEtlContextLocalStateSqliteDb";

import { dbCols } from "./data_schema";

export type InitialEvent = {
  type: ":INITIAL";
  payload: {
    etl_work_dir: string;
  };
};

export type FinalEvent = {
  type: ":FINAL";
};

async function moveStagedTranscomEventsToPublished(staging_schema: string) {
  const indent = "".repeat(10);

  const conflictActionsHolders = dbCols
    .map(() => `${indent}%I = EXCLUDED.%I`)
    .join(",\n");

  const conflictActionFillers = dbCols.reduce((acc: string[], col) => {
    acc.push(col);
    acc.push(col);
    return acc;
  }, []);

  const sql = dedent(
    pgFormat(
      `
        INSERT INTO _transcom_admin.transcom_events_expanded
          SELECT
              *
            FROM %I.transcom_events_expanded
            ON CONFLICT ON CONSTRAINT transcom_events_expanded_pkey
              DO UPDATE
                SET -- SEE: https://stackoverflow.com/a/40689501/3970755
                  ${conflictActionsHolders}
        ; 
      `,
      staging_schema,
      ...conflictActionFillers
    )
  );

  logger.silly(sql);

  await dama_db.query(sql);
}

// NOTE:  Should not be run as a Queued Subtask because will need to be
//        in the same TRANSACTION as post-processing tasks.
export default async function main(etl_work_dir: string) {
  verifyIsInTaskEtlContext();

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

  await moveStagedTranscomEventsToPublished(staging_schema);
}
