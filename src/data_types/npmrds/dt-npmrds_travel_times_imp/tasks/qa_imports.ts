import dedent from "dedent";
import pgFormat from "pg-format";

import dama_db from "data_manager/dama_db";

import { NpmrdsDataSources } from "../../domain";

// The ratio of TMCs from the TMC_Identification files that MUST appear in the travel times data.
const TRAVEL_TIMES_DATA_COVERAGE_THRESHOLD = 0.9;
const TMC_IDENTIFICATION_COUNT_COVERAGE_THRESHOLD = 0.95;

// NOTE: There are no QA stats for Travel Times data prior to 2023-01-01 because
//       the tables were loaded prior to the DataManager ETL process.
//       If such statistics become required, we could backfill the statistics.
export async function getQAStatistics(
  view_ids: number | number[],
  lookback_num = 5
) {
  const statistic_sql = dedent(`
    WITH cte_new_views_stats AS (
      SELECT
          view_id,
          geography_version,
          start_date,
          upload_date,
          table_name,
          num_days,
          SUM((frc_stats->'total_tmcs')::NUMERIC)::INTEGER AS total_tmcs,
          ROUND(
            (
              SUM((frc_stats->'total_records')::NUMERIC)
              / SUM((frc_stats->'total_tmcs')::NUMERIC)
            ) / num_days,
            3
          )::DOUBLE PRECISION AS total_records_per_tmc_per_day
        FROM (
          SELECT
              view_id,
              geography_version,
              start_date,
              upload_date,
              table_name,
              num_days,
              (jsonb_each(stats)).value AS frc_stats
          FROM (
              SELECT
                  view_id,
                  geography_version,
                  start_date,
                  _created_timestamp::DATE AS upload_date,
                  table_name,
                  ( end_date - start_date + 1 ) AS num_days,
                  statistics->'summary_by_frc' AS stats
              FROM data_manager.views
              WHERE ( view_id = ANY($1) )
          ) AS x
      ) AS y
      GROUP BY 1,2,3,4,5,6
    )
      SELECT
          *
        FROM (
            SELECT
                geography_version                   AS geography_version,

                a.view_id                           AS new_view_id,
                a.table_name                        AS new_table_name,
                a.num_days                          AS new_num_days,
                a.total_tmcs                        AS new_total_tmcs,
                a.total_records_per_tmc_per_day     AS new_total_records_per_tmc_per_day,

                b.view_id                           AS auth_view_id,
                b.table_name                        AS auth_table_name,
                b.num_days                          AS auth_num_days,
                b.total_tmcs                        AS auth_total_tmcs,
                b.total_records_per_tmc_per_day     AS auth_total_records_per_tmc_per_day,

                row_number() OVER (
                    PARTITION BY a.view_id
                    ORDER BY
                    -- Prefer same year
                    (
                      ABS(
                        EXTRACT(YEAR FROM a.start_date)
                        - EXTRACT(YEAR FROM b.start_date)
                      )
                    ),
                    ABS(a.start_date - b.start_date)
                ) AS row_num

              FROM cte_new_views_stats AS a
                INNER JOIN (
                  SELECT
                      view_id,
                      geography_version,
                      start_date,
                      upload_date,
                      table_name,
                      num_days,
                      SUM((frc_stats->'total_tmcs')::NUMERIC)::INTEGER AS total_tmcs,
                      ROUND(
                        (
                          SUM((frc_stats->'total_records')::NUMERIC)
                          / SUM((frc_stats->'total_tmcs')::NUMERIC)
                        ) / num_days,
                        3
                      )::DOUBLE PRECISION AS total_records_per_tmc_per_day
                    FROM (
                      SELECT
                          view_id,
                          geography_version,
                          start_date,
                          upload_date,
                          table_name,
                          num_days,
                          (jsonb_each(stats)).value AS frc_stats -- NOTE: If stats is NULL, no rows created.
                        FROM (
                          SELECT
                              view_id,
                              geography_version,
                              start_date,
                              _created_timestamp::DATE AS upload_date,
                              table_name,
                              ( end_date - start_date + 1 ) AS num_days,
                              statistics->'summary_by_frc' AS stats
                          FROM data_manager.views AS a
                            INNER JOIN (
                              SELECT
                                  UNNEST(x.view_dependencies) AS view_id
                                FROM data_manager.views AS x
                                  INNER JOIN data_manager.sources AS y
                                    USING (source_id)
                                WHERE (
                                  ( x.active_end_timestamp IS NULL )
                                  AND
                                  ( y.name = $2 )
                                )
                            ) AS b USING (view_id)
                            INNER JOIN (
                              SELECT DISTINCT
                                  geography_version
                                FROM cte_new_views_stats
                            ) AS c USING ( geography_version )
                        ) AS x
                   ) AS y
                   GROUP BY 1,2,3,4,5,6
                ) AS b USING (geography_version)
        ) AS t
        WHERE ( row_num <= $3 )
  `);

  const { rows: qa_statistics } = await dama_db.query({
    text: statistic_sql,
    values: [view_ids, NpmrdsDataSources.NpmrdsTravelTimes, lookback_num],
  });

  return qa_statistics;
}

async function qaDataCompleteness(qa_statistics: any) {
  const new_view_ids = [
    ...new Set(qa_statistics.map(({ new_view_id }) => new_view_id)),
  ];

  const us_state_view_ids_sql = dedent(`
    SELECT
        a.view_id
      FROM data_manager.views AS a
        INNER JOIN public.fips_codes AS b
          ON ( a.geography_version = b.state_code )
      WHERE (
        ( a.view_id = ANY($1) )
        AND
        ( b.country = 'us' )
      )
  `);

  const { rows: us_state_view_ids_res } = await dama_db.query({
    text: us_state_view_ids_sql,
    values: [new_view_ids],
  });

  const us_state_view_ids = new Set(
    us_state_view_ids_res.map(({ view_id }) => view_id)
  );

  const actual_coverage_by_new_view_id: Record<number, number> = {};
  const threshold_coverage_by_new_view_id: Record<number, number> = {};

  for (const {
    new_view_id,
    new_total_records_per_tmc_per_day,
    auth_total_records_per_tmc_per_day,
  } of qa_statistics) {
    actual_coverage_by_new_view_id[new_view_id] =
      new_total_records_per_tmc_per_day;

    threshold_coverage_by_new_view_id[new_view_id] = Math.min(
      threshold_coverage_by_new_view_id[new_view_id] || Infinity,
      auth_total_records_per_tmc_per_day * TRAVEL_TIMES_DATA_COVERAGE_THRESHOLD
    );
  }

  const error_messages = [] as string[];

  console.log({
    actual_coverage_by_new_view_id,
    threshold_coverage_by_new_view_id,
  });

  for (const view_id of Object.keys(actual_coverage_by_new_view_id)) {
    const actual = actual_coverage_by_new_view_id[view_id];
    const threshold = threshold_coverage_by_new_view_id[view_id];

    if (actual < threshold) {
      error_messages.push(
        `ERROR: For view_id=${view_id}, actual total_records_per_tmc_per_day ${actual} is less than the threshold ${threshold}.`
      );
    }
  }

  return error_messages;
}

async function qaDateCounts(qa_statistics: any) {
  interface stats {
    new_table_name: string;
    new_num_days: number;
  }

  const stats_summary: Record<number, stats> = qa_statistics.reduce(
    (acc: any, { new_view_id, new_table_name, new_num_days }) => {
      acc[new_view_id] = {
        new_table_name,
        new_num_days,
      };

      return acc;
    },
    {}
  );

  const new_view_date_counts_sql = Object.entries(stats_summary)
    .map(([k, { new_table_name }]) =>
      dedent(
        pgFormat(
          `
            SELECT
                %s AS travel_times_view_id,
                COUNT(DISTINCT date)::INTEGER AS expected_date_count
              FROM %I.%I
          `,
          k,
          "npmrds_travel_times_imports",
          new_table_name
        )
      )
    )
    .join("\nUNION ALL\n");

  const { rows: new_view_date_counts } = await dama_db.query(
    new_view_date_counts_sql
  );

  console.table(new_view_date_counts);

  const error_messages = [] as string[];

  for (const {
    travel_times_view_id,
    expected_date_count,
  } of new_view_date_counts) {
    const actual_date_count = stats_summary[travel_times_view_id].new_num_days;

    if (expected_date_count !== actual_date_count) {
      error_messages.push(
        `ERROR: For view_id=${travel_times_view_id}, actual date count ${actual_date_count} does not equal expected date count ${expected_date_count}.`
      );
    }
  }

  return error_messages;
}

async function qaExpectedTmcCounts(qa_statistics: any) {
  interface stats {
    tmc_ident_table_name: string;
    new_total_tmcs: number;
  }

  const stats_summary: Record<number, stats> = qa_statistics.reduce(
    (acc: any, { new_view_id, new_table_name, new_total_tmcs }) => {
      const tmc_ident_table_name = new_table_name
        .replace(/^npmrdsx?/, "tmc_identification")
        .replace(/_from/, "")
        .replace(/\d{4}_to_\d{8}/, "");

      acc[new_view_id] = {
        tmc_ident_table_name,
        new_total_tmcs,
      };

      return acc;
    },
    {}
  );

  const tmc_ident_counts_sql = Object.entries(stats_summary)
    .map(([k, { tmc_ident_table_name }]) =>
      dedent(
        pgFormat(
          `
            SELECT
                %s AS travel_times_view_id,
                COUNT(DISTINCT tmc)::INTEGER AS tmc_ident_tmc_count
              FROM %I.%I
              WHERE ( UPPER(country) != 'CANADA' )  -- OMIT Canada
          `,
          k,
          "npmrds_tmc_identification_imports",
          tmc_ident_table_name
        )
      )
    )
    .join("\nUNION ALL\n");

  const { rows: tmc_ident_counts } = await dama_db.query(tmc_ident_counts_sql);

  console.table(tmc_ident_counts);

  const error_messages = [] as string[];

  for (const {
    travel_times_view_id,
    tmc_ident_tmc_count,
  } of tmc_ident_counts) {
    const expected_tmc_count =
      TMC_IDENTIFICATION_COUNT_COVERAGE_THRESHOLD * tmc_ident_tmc_count;

    const { new_total_tmcs: actual_tmc_count } =
      stats_summary[travel_times_view_id];

    if (actual_tmc_count < expected_tmc_count) {
      error_messages.push(
        `ERROR: For view_id=${travel_times_view_id}, actual tmc count ${actual_tmc_count} does not equal expected tmc count ${expected_tmc_count}.`
      );
    }
  }

  return error_messages;
}

export default async function getQAErrorMessages(
  view_ids: number | number[],
  lookback_num = 5
) {
  console.log(`QA Travel Times Imports ${view_ids}`);
  console.group();
  const qa_statistics = await getQAStatistics(view_ids, lookback_num);

  console.table(qa_statistics);

  const qa_coverage_err_msgs = await qaDataCompleteness(qa_statistics);

  const qa_date_err_msgs = await qaDateCounts(qa_statistics);

  const qa_tmc_count_err_msgs = await qaExpectedTmcCounts(qa_statistics);

  const err_msgs = [
    ...qa_coverage_err_msgs,
    ...qa_date_err_msgs,
    ...qa_tmc_count_err_msgs,
  ];

  console.log("ERROR Messages:", err_msgs);

  console.groupEnd();

  return err_msgs.length ? err_msgs : null;
}
