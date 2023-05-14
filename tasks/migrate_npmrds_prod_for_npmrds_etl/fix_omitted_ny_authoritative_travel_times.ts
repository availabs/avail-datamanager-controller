/*
psql -dnpmrds_production -f sql/get_omitted_ny_npmrds_travel_time_imports.sql

 view_id |        table_schema         |                      table_name
---------+-----------------------------+-------------------------------------------------------
     697 | npmrds_travel_times_imports | npmrdsx_ny_from_20220601_to_20220630_v00000000t000000
     698 | npmrds_travel_times_imports | npmrdsx_ny_from_20220701_to_20220731_v00000000t000000
     699 | npmrds_travel_times_imports | npmrdsx_ny_from_20220801_to_20220831_v00000000t000000
     700 | npmrds_travel_times_imports | npmrdsx_ny_from_20220901_to_20220930_v00000000t000000
     701 | npmrds_travel_times_imports | npmrdsx_ny_from_20221001_to_20221031_v00000000t000000
     702 | npmrds_travel_times_imports | npmrdsx_ny_from_20221101_to_20221130_v00000000t000000
     703 | npmrds_travel_times_imports | npmrdsx_ny_from_20221201_to_20221231_v00000000t000000
     704 | npmrds_travel_times_imports | npmrdsx_ny_from_20230101_to_20230131_v00000000t000000
     705 | npmrds_travel_times_imports | npmrdsx_ny_from_20230201_to_20230228_v00000000t000000
     706 | npmrds_travel_times_imports | npmrdsx_ny_from_20230301_to_20230331_v00000000t000000
     707 | npmrds_travel_times_imports | npmrdsx_ny_from_20230401_to_20230430_v00000000t000000
(11 rows)
*/

import { runInDamaContext } from "../../src/data_manager/contexts";

import makeTravelTimesExportTablesAuthoritative from "../../src/data_types/npmrds/dt-npmrds_travel_times/actions/makeTravelTimesExportTablesAuthoritative";

import { PG_ENV } from "./domain";

async function main() {
  const ctx = { meta: { pgEnv: PG_ENV } };

  await runInDamaContext(ctx, async () => {
    await makeTravelTimesExportTablesAuthoritative([
      697, 698, 699, 700, 701, 702, 703, 704, 705, 706, 707,
    ]);
  });
}

main();
