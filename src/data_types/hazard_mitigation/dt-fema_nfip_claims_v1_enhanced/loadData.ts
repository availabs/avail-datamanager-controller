import { Context } from "moleculer";
import {err, fin, init, update_view} from "../utils/macros";

export default async function publish(ctx: Context) {
  let {
    // @ts-ignore
    params: { table_name, nfip_schema, nfip_table, dds_schema, dds_table, county_schema, county_table },
  } = ctx;

  const {etl_context_id, dbConnection, source_id, view_id, sqlLog} = await init({ctx, type: table_name});

  try {
    const sql = `
    with disasters as (
                       SELECT disaster_number::text,
                              incident_type,
                              fips_state_code || fips_county_code          geoid,
                              MIN(incident_begin_date)					           incident_begin_date,
                              MAX(incident_end_date)						           incident_end_date
                       FROM ${dds_schema}.${dds_table}
                       WHERE disaster_number NOT BETWEEN 3000 and 3999
                         AND incident_type in ('Coastal Storm', 'Dam/Levee Break', 'Flood', 'Hurricane', 'Severe Storm', 'Severe Storm(s)', 'Tornado', 'Tsunami', 'Typhoon')
                       GROUP BY 1, 2, 3
                       ORDER BY 1 DESC
                  ),
        		enhanced_geoids as (
                      SELECT
                        geoid, date_of_loss,
                        amount_paid_on_contents_claim,
                        amount_paid_on_building_claim,
                        amount_paid_on_increased_cost_of_compliance_claim
                      FROM ${nfip_schema}.${nfip_table} nfip
                      LEFT JOIN ${county_schema}.${county_table} county
                      ON st_contains(county.geom, st_setsrid(st_makepoint(longitude, latitude), 4326))
                      where county_code is null
                      and latitude + longitude is not null

                      UNION ALL

                      SELECT
                        county_code as geoid, date_of_loss,
                        amount_paid_on_contents_claim,
                        amount_paid_on_building_claim,
                        amount_paid_on_increased_cost_of_compliance_claim
                      FROM ${nfip_schema}.${nfip_table} nfip
                      where county_code is not null
		            ),
         nfip as (
             SELECT disaster_number::text,
                    nfip.geoid,
                    incident_type,
                    COALESCE(SUM(amount_paid_on_contents_claim), 0) +
                    COALESCE(SUM(amount_paid_on_building_claim), 0) +
                    COALESCE(SUM(amount_paid_on_increased_cost_of_compliance_claim), 0) total_amount_paid
             FROM enhanced_geoids nfip
             JOIN disasters dd
                    ON nfip.geoid = dd.geoid
                    AND date_of_loss BETWEEN incident_begin_date AND incident_end_date
             GROUP BY 1, 2, 3
         )

         SELECT * INTO ${nfip_schema}.${table_name}_${view_id} FROM nfip;
    `;
    console.log(sql)
    await ctx.call("dama_db.query", {text: sql});
    await update_view({table_schema: 'open_fema_data', table_name, view_id, dbConnection, sqlLog});

    return fin({etl_context_id, ctx, dbConnection, payload: {view_id, source_id}});
  } catch (e) {
    return err({e, etl_context_id, sqlLog, ctx, dbConnection});
  }
}
