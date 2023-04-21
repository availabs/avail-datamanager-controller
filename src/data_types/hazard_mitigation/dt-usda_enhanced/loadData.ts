import { Context } from "moleculer";
import { PoolClient, QueryConfig, QueryResult } from "pg";
import {FSA} from "flux-standard-action";
import dedent from "dedent";
import pgFormat from "pg-format";
import EventTypes from "../constants/EventTypes";
import {tables} from "./tables";
import fs from "fs";
import https from "https";
import {execSync} from "child_process";
import {loadFiles} from "./upload";
import {err, fin, init, update_view} from "../utils/macros";

const fetchFileList = async (currTable, from, to) => {
  console.log("fetching...");

  if(!fs.existsSync("data/" + currTable)){
    fs.mkdirSync("data/" + currTable);
  }

  let years  = [];
  let promises = []

  for ( let year = from; year <= to; year++){ years.push(year) }

    years.forEach(async (year) => {
      console.log('year', year)

      const url = `https://www.rma.usda.gov/-/media/RMA/Cause-Of-Loss/Summary-of-Business-with-Month-of-Loss/colsom_${year}.ashx?la=en`;
      const file_path = "data/" + currTable + "/" + `${year}.zip`;
      const command = `zcat ${file_path} > ${file_path.replace('.zip', '.txt')}`
      const file = fs.createWriteStream(file_path);

      promises.push(
        new Promise((resolve) => {
          https.get(url, response => {
            response.pipe(file);
            console.log('got: ', url);
            file.on('finish', f => {
              file.close();

              file.once('close', () => {
                file.removeAllListeners();
              });

              resolve(execSync(command, {encoding: 'utf-8'}));
            });

          })
        })
      );
    });

  return Promise.all(promises).then(() => console.log('unzipped'))


  // return years.reduce(async (acc, year) => {
  //   console.log('unzipping', year)
  //   await acc;
  //   console.log('unzipping 1', year)
  //   const file_path = "data/" + currTable + "/" + `${year}.zip`;
  //   const command = `zcat ${file_path} > ${file_path.replace('.zip', '.txt')}`
  //   execSync(command, {encoding: 'utf-8'});
  //
  // }, Promise.resolve());

}

export default async function publish(ctx: Context) {
  // throw new Error("publish TEST ERROR");

  let {
    // @ts-ignore
    params: { table_name, usda_schema, usda_table, dds_schema, dds_table },
  } = ctx;

  const {etl_context_id, dbConnection, source_id, view_id, sqlLog} = await init({ctx, type: table_name});

  try {
    const sql = `
    with disasters as (
                       SELECT disaster_number::text,
                              incident_type,
                              fips_state_code || fips_county_code          geoid,
                              MIN(incident_begin_date)					           incident_begin_date,
                              MAX(incident_end_date)						           incident_end_date,
                              EXTRACT(YEAR FROM MIN(incident_begin_date))  begin_year,
                              EXTRACT(YEAR FROM MAX(incident_end_date))    end_year,
                              EXTRACT(MONTH FROM MIN(incident_begin_date)) begin_month,
                              EXTRACT(MONTH FROM MAX(incident_end_date))   end_month
                       FROM ${dds_schema}.${dds_table}
                       WHERE disaster_number NOT BETWEEN 3000 and 3999
                       GROUP BY 1, 2, 3
                       ORDER BY 1 DESC
                  ),
         	croploss_raw as (
                        SELECT state_fips || county_fips   as geoid,
                          commodity_year_identifier::int as year,
                          month_of_loss::int 			   as month,
                          CASE
                            WHEN cause_of_loss_desc IN ('Excess Moisture/Precipitation/Rain', 'Flood')
                              THEN 'Flood'
                            WHEN cause_of_loss_desc IN ('Hail')
                              THEN 'Hail'
                            WHEN cause_of_loss_desc IN ('Storm Surge')
                              THEN 'Coastal Storm'
                            WHEN cause_of_loss_desc IN ('Hurricane/Tropical Depression')
                              THEN 'Hurricane'
                            WHEN cause_of_loss_desc IN ('Tidal Wave/Tsunami')
                              THEN 'Tsunami'
                            WHEN cause_of_loss_desc IN ('Tornado')
                              THEN 'Tornado'
                            WHEN cause_of_loss_desc LIKE '%Snow%'
                            -- ('"Other (Snow,Lightning,etc)"','Other (Snow,Lightning,etc)','Other (Snow,Lightning,etc)','Other (Snow, Lightning, Etc.)','Other (Volcano,Snow,Lightning,etc)', '"Other (Volcano,Snow,Lightning,etc)"', 'Other (Volcano,Snow,Lightning,etc)')
                              THEN 'Snow'
                            WHEN cause_of_loss_desc IN ('Freeze','Cold Winter','Cold Wet Weather','Frost','Ice Flow','Ice Floe','Ice floe')
                              THEN 'Freezing'
                            WHEN cause_of_loss_desc IN ('Cyclone')
                              THEN 'Typhoon'
                            WHEN cause_of_loss_desc IN ('Earthquake')
                              THEN 'Earthquake'
                            WHEN cause_of_loss_desc IN ('Volcanic Eruption')
                              THEN 'Volcano'
                            WHEN cause_of_loss_desc IN ('Force Fire','House burn (Pole burn)','Fire','Pit Burn','House Burn (Pole Burn)')
                              THEN 'Fire'
                            WHEN cause_of_loss_desc IN ('Drought','Drought Deviation')
                              THEN 'Drought'
                            WHEN cause_of_loss_desc IN ('Hot Wind', 'Heat')
                                        THEN 'Heat'
                            WHEN cause_of_loss_desc IN ('Wind/Excess Wind')
                                        THEN 'Wind'
                            ELSE cause_of_loss_desc
                            END cause_of_loss_desc,
                          SUM(indemnity_amount) indemnity_amount
                         FROM ${usda_schema}.${usda_table}
                         WHERE cause_of_loss_desc in (
                                'Cold Wet Weather', 'Cold Winter', 'Freeze', 'Frost', 'Hail', 'Ice floe', 'Ice Floe', 'Ice Flow',
                                '"Other (Snow,Lightning,etc)"','Other (Snow,Lightning,etc)','Other (Snow, Lightning, Etc.)',
                                '"Other (Volcano,Snow,Lightning,etc)"', 'Other (Volcano,Snow,Lightning,etc)',
                                'Drought', 'Drought Deviation', 'Earthquake', 'Volcanic Eruption',
                                'Excess Sun', 'Heat', 'Hot Wind', 'House burn (Pole burn)', 'House Burn (Pole Burn)',
                                'Fire', 'Force Fire', 'Pit Burn', 'Wind/Excess Wind',
                                'Excess Moisture/Precipitation/Rain', 'Flood', 'Storm Surge', 'Tidal Wave/Tsunami',
                                'Tornado', 'Hurricane/Tropical Depression', 'Cyclone'
                                )
                        AND month_of_loss != ''
                        GROUP BY 1, 2, 3, 4
                        HAVING SUM(indemnity_amount) > 0
                        ),
          croploss as (
            SELECT disaster_number, d.geoid, incident_type, sum(indemnity_amount) crop_loss
            FROM croploss_raw usda
            JOIN disasters d
            ON usda.geoid = d.geoid
            AND usda.cause_of_loss_desc = d.incident_type
            AND usda.year BETWEEN d.begin_year AND d.end_year
            AND usda.month BETWEEN d.begin_month AND d.end_month
            GROUP BY 1, 2, 3
          )

         SELECT * INTO ${usda_schema}.${table_name}_${view_id} FROM croploss;
    `;
    console.log(sql)
    await ctx.call("dama_db.query", {text: sql});
    // update view meta
    await update_view({table_schema: usda_schema, table_name, view_id, dbConnection, sqlLog});

    return fin({etl_context_id, ctx, dbConnection, payload: {view_id, source_id}});
  } catch (e) {
    return err({e, etl_context_id, sqlLog, ctx, dbConnection});
  }
}
