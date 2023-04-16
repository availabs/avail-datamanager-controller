import { createReadStream } from "fs";
import { createGunzip } from "zlib";
import { pipeline } from "stream";
import { promisify } from "util";

import { format as csvFormat, CsvFormatterStream } from "fast-csv";
import _ from "lodash";
import split from "split2";
import pgFormat from "pg-format";
import dedent from "dedent";
import { from as copyFrom } from "pg-copy-streams";

import dama_db from "data_manager/dama_db";
import dama_events from "data_manager/events";
import logger from "data_manager/logger";

import { verifyIsInTaskEtlContext } from "data_manager/contexts";

import getEtlContextLocalStateSqliteDb from "../../utils/getEtlContextLocalStateSqliteDb";
import { getRawTranscomEventsFilePath } from "../../utils/etlWorkDir";

import initialize_database_tables from "../initialize_database_tables";

// NOTE: dbCols array ensures same order of columns in CSV and COPY FROM CSV statement.
import { apiResponsePropsToDbCols, dbCols } from "./data_schema";

import {
  RawTranscomEventExpanded,
  ProtoTranscomEventExpanded,
} from "../../domain";

export type InitialEvent = {
  type: ":INITIAL";
  payload: {
    etl_work_dir: string;
  };
  meta: { subtask_name: "load_transcom_events" };
};

export type FinalEvent = {
  type: ":FINAL";
};

const pipelineAsync = promisify(pipeline);

export function transformRawTranscomEventExpandedToProtoTranscomEventExpanded(
  e: RawTranscomEventExpanded
): ProtoTranscomEventExpanded {
  // TODO: TEST
  return <ProtoTranscomEventExpanded>(
    _.mapKeys(e, (_v, k) => apiResponsePropsToDbCols[k])
  );
}

export async function* transformRawTranscomEventExpandedIteratorToProtoTranscomEventExpandedIterator(
  rawTranscomEventExpandedIter:
    | Iterable<RawTranscomEventExpanded>
    | AsyncIterable<RawTranscomEventExpanded>
): AsyncGenerator<ProtoTranscomEventExpanded> {
  for await (const event of rawTranscomEventExpandedIter) {
    yield transformRawTranscomEventExpandedToProtoTranscomEventExpanded(event);
  }
}

export function protoTranscomEventExpandedIteratorToCsvStream(
  protoTranscomEventExpandedIter: AsyncGenerator<ProtoTranscomEventExpanded>
): CsvFormatterStream<ProtoTranscomEventExpanded, any> {
  const csv_stream = csvFormat({
    headers: dbCols,
    quoteHeaders: false,
    quote: '"',
  });

  process.nextTick(async () => {
    for await (const event of protoTranscomEventExpandedIter) {
      const ready = csv_stream.write(event);

      if (!ready) {
        await new Promise((resolve) => csv_stream.once("drain", resolve));
        console.error("drain event");
      }
    }

    csv_stream.end();
  });

  return csv_stream;
}

export function makeRawTranscomEventExpandedIteratorFromApiScrapeFile(
  filePath: string
): AsyncGenerator<RawTranscomEventExpanded> {
  // @ts-ignore
  return pipeline(
    createReadStream(filePath),
    createGunzip(),
    split(JSON.parse),
    (err) => {
      if (err) {
        logger.error((<Error>err).message);
        logger.error((<Error>err).stack);
        throw err;
      }
    }
  );
}

// NOTE: Assumes file naming pattern /^raw-transcom-events\.*\.ndjson.gz$/
export async function* makeRawTranscomEventsExpandedIteratorFromApiScrapeDirectory(
  etl_work_dir: string
) {
  const sqlite_db = getEtlContextLocalStateSqliteDb(etl_work_dir);

  const file_paths_iter = sqlite_db
    .prepare(
      `
        SELECT DISTINCT
           file_name
          FROM downloaded_event
          ORDER BY 1
      `
    )
    .pluck()
    .iterate();

  for (const file_name of file_paths_iter) {
    const file_path = getRawTranscomEventsFilePath(etl_work_dir, file_name);

    const rawEventsIter =
      makeRawTranscomEventExpandedIteratorFromApiScrapeFile(file_path);

    for await (const event of rawEventsIter) {
      yield event;
    }
  }
}

export async function createTranscomEventsExpandedStagingTable(
  staging_schema: string
) {
  const sql = dedent(
    pgFormat(
      `
        CREATE SCHEMA IF NOT EXISTS %I ;

        CREATE TABLE IF NOT EXISTS %I.transcom_events_expanded (
          LIKE _transcom_admin.transcom_events_expanded
            INCLUDING DEFAULTS      -- necessary for _created_timestamp & _modified_timestamp
            EXCLUDING CONSTRAINTS   -- because scrapes may violate PrimaryKey CONSTRAINT
        ) ;
      `,
      staging_schema,
      staging_schema
    )
  );

  logger.silly(sql);

  await dama_db.query(sql);

  await dama_events.dispatch({
    type: ":CREATED_TRANSCOM_EVENTS_EXPANDED_STAGING_TABLE",
    // @ts-ignore
    payload: {
      staging_schema,
    },
  });
}

export async function loadProtoTranscomEventsExpandedIntoDatabase(
  protoTranscomEventIter: AsyncGenerator<ProtoTranscomEventExpanded>,
  staging_schema: string
) {
  await createTranscomEventsExpandedStagingTable(staging_schema);

  // NOTE: using the transcomEventsDatabaseTableColumns array
  //       keeps column order consistent with transcomEventsCsvStream
  const column_identifiers = dbCols.slice().fill("%I").join();

  const sql = pgFormat(
    `COPY %I.transcom_events_expanded (${column_identifiers}) FROM STDIN WITH CSV HEADER ;`,
    staging_schema,
    ...dbCols
  );

  const db = await dama_db.getDbConnection();

  try {
    const pg_copy_stream = db.query(copyFrom(sql));

    const csv_stream = protoTranscomEventExpandedIteratorToCsvStream(
      protoTranscomEventIter
    );

    await pipelineAsync(csv_stream, pg_copy_stream);
  } catch (err) {
    logger.error((<Error>err).message);
    logger.error((<Error>err).stack);
    throw err;
  } finally {
    db.release();
  }
}

export default async function main(etl_work_dir: string) {
  verifyIsInTaskEtlContext();

  const events = await dama_events.getAllEtlContextEvents();

  let final_event = events.find(({ type }) => type === ":FINAL");

  if (final_event) {
    return final_event;
  }

  await initialize_database_tables();

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

  const rawEventsIter =
    makeRawTranscomEventsExpandedIteratorFromApiScrapeDirectory(etl_work_dir);

  const protoTranscomEventIter =
    transformRawTranscomEventExpandedIteratorToProtoTranscomEventExpandedIterator(
      rawEventsIter
    );

  await loadProtoTranscomEventsExpandedIntoDatabase(
    protoTranscomEventIter,
    staging_schema
  );

  final_event = { type: ":FINAL" };

  await dama_events.dispatch(final_event);

  return final_event;
}
