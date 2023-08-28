import { groupBy, map, mapValues } from "lodash";
import fetch from "node-fetch";

import logger from "data_manager/logger";
import dama_events from "data_manager/events";
import dama_db from "data_manager/dama_db";

export default async function publish({
  years,
  view_id,
  counties,
  serverUrl,
  source_id,
  variables,
  viewDependency,
  etl_context_id: etlContextId,
}) {
  const res = await dama_db.query({
    text: "select * from data_manager.views where view_id = $1",
    values: [viewDependency],
  });

  const { data_table } = res.rows[0];

  logger.info(`New Data table fetched: ${data_table}`);

  const { rows } = await dama_db.query({
    text: ` SELECT DISTINCT geoid, year
    FROM ${data_table}
    WHERE year = ANY($1::INT[])
    AND tiger_type = ANY($2::TEXT[])
    AND geoid LIKE ANY(
      ARRAY(
        SELECT u || '%'
        FROM UNNEST($3::TEXT[]) AS t(u)
      )
    )`,
    values: [years, ["tract", "county"], counties],
  });

  logger.info(`Get Geoid query fetched successfully \n and Rows are: ${rows}`);
  const groupByCounties = mapValues(groupBy(rows, "year"), (geoidArray) =>
    map(geoidArray, "geoid")
  );

  logger.info(`new groupBy: ${JSON.stringify(groupByCounties, null, 3)}`);
  const censusVars = variables?.reduce(
    (
      acc: Array<string>,
      cur: Record<
        string,
        {
          label: string;
          censusKeys: string[];
          divisorKeys: string[];
        }
      >
    ) => {
      const {
        value: { censusKeys, divisorKeys },
      } = cur;

      acc = [...acc, ...(censusKeys || []), ...(divisorKeys || [])];
      return acc;
    },
    []
  );

  logger.info("Ready for the Execution");
  const size = 300;
  for (const [, year] of years.entries()) {
    logger.info(`Execution started for the year: ${year}`);
    const geoYear = year - (year % 10);
    const tempGeos = groupByCounties[`${geoYear}`];
    const newEvent = {
      type: `CACHE_ACS: EXECUTION_STARTED_FOR_${year}`,
      payload: {},
      meta: {
        timestamp: new Date().toISOString(),
      },
    };
    logger.info(`\nUpload Event ${JSON.stringify(newEvent, null, 3)}`);
    await dama_events.dispatch(newEvent, etlContextId);
    for (let i = 0; i < tempGeos.length; i += size) {
      logger.info(
        `\n\n\n----------  YEAR: ${year} New Chunk: ${
          i / size
        } ----------------- \n\n\n`
      );
      const chunk = tempGeos.slice(i, i + size);
      const path = [["acs", chunk, year, censusVars]];
      const urlString = `${serverUrl}?paths=${encodeURIComponent(
        JSON.stringify(path)
      )}&method=get`;
      await fetch(urlString);
    }
  }
  const finalEvent = {
    type: "CACHE_ACS:FINAL",
    payload: {
      damaSourceId: source_id,
    },
    meta: {
      timestamp: new Date().toISOString(),
    },
  };

  await dama_events.dispatch(finalEvent, etlContextId);
  return finalEvent;
}
