BEGIN ;

INSERT INTO all_tmc_timestamp_pairs (
  tmc_code,
  measurement_tstamp
)
  SELECT
      tmc_code,
      measurement_tstamp
    FROM all_vehicles
  UNION
  SELECT
      tmc_code,
      measurement_tstamp
    FROM passenger_vehicles
  UNION
  SELECT
      tmc_code,
      measurement_tstamp
    FROM trucks
;

INSERT INTO npmrds_travel_times (
  tmc,
  date,
  epoch,
  travel_time_all_vehicles,
  travel_time_passenger_vehicles,
  travel_time_freight_trucks,
  data_density_all_vehicles,
  data_density_passenger_vehicles,
  data_density_freight_trucks
)
  SELECT
      tmc,
      date,
      epoch,

      COALESCE(
        travel_time_all_vehicles,
        travel_time_passenger_vehicles,
        travel_time_freight_trucks
      ) AS travel_time_all_vehicles,
      travel_time_passenger_vehicles,
      travel_time_freight_trucks,

      COALESCE(
        data_density_all_vehicles,
        data_density_passenger_vehicles,
        data_density_freight_trucks
      ) AS data_density_all_vehicles,
      data_density_passenger_vehicles,
      data_density_freight_trucks

    FROM (
      SELECT
          tmc_code AS tmc,

          strftime('%Y%m%d', measurement_tstamp) AS date,

          (
            (
              CAST(strftime('%H', measurement_tstamp) AS INTEGER)
              * 12
            )
            +
            (
              CAST(strftime('%M', measurement_tstamp) AS INTEGER)
              / 5
            )
          ) AS epoch,

          -- NOTE: The formatting is necessary to get the exact same CSV as the legacy ETL process.
          --       Basically, it removes trailing zeros from digits. (EG: 2.50 -> 2.5)
          CASE
            WHEN (a.travel_time_seconds IS NOT NULL)
              THEN
                rtrim(
                  rtrim(
                    printf('%f', a.travel_time_seconds),
                    '0'
                  ),
                  '.'
                )
              ELSE NULL
          END AS travel_time_all_vehicles,

          CASE
            WHEN (b.travel_time_seconds IS NOT NULL)
              THEN
                rtrim(
                  rtrim(
                    printf('%f', b.travel_time_seconds),
                    '0'
                  ),
                  '.'
                )
              ELSE NULL
          END AS travel_time_passenger_vehicles,

          CASE
            WHEN (c.travel_time_seconds IS NOT NULL)
              THEN
                rtrim(
                  rtrim(
                    printf('%f', c.travel_time_seconds),
                    '0'
                  ),
                  '.'
                )
              ELSE NULL
          END AS travel_time_freight_trucks,

          a.data_density AS data_density_all_vehicles,
          b.data_density AS data_density_passenger_vehicles,
          c.data_density AS data_density_freight_trucks

        FROM all_tmc_timestamp_pairs
          LEFT OUTER JOIN all_vehicles AS a
            USING (tmc_code, measurement_tstamp)
          LEFT OUTER JOIN passenger_vehicles AS b
            USING (tmc_code, measurement_tstamp)
          LEFT OUTER JOIN trucks AS c
            USING (tmc_code, measurement_tstamp)
        ORDER BY 1, 2, 3
    ) AS t
;

COMMIT ;
