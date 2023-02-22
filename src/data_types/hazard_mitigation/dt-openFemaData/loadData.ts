import { Context } from "moleculer";
import {chunk} from "lodash";
import sql from "sql";
import Promise from "bluebird";
import fetch from "node-fetch";
import tables from "./tables";
import {err, fin, init, update_view} from "../utils/macros";

sql.setDialect("postgres");

// mol $ call "dama/data_source_integrator.testUploadAction" --table_name details --#pgEnv dama_dev_1
const url = "https://www.fema.gov/api/open/v1/OpenFemaDataSets"
const camelToSnakeCase = (str = "") => str.replace(/[A-Z]/g, letter => `_${letter.toLowerCase()}`);


const datasets = [
  "data_set_fields_v1",
  "data_sets_v1",
  "disaster_declarations_summaries_v1",
  "disaster_declarations_summaries_v2",
  "emergency_management_performance_grants_v1",
  "fema_regions_v1",
  "fema_regions_v2",
  "fema_web_declaration_areas_v1",
  "fema_web_disaster_declarations_v1",
  "fema_web_disaster_summaries_v1",
  "fima_nfip_claims_v1",
  "fima_nfip_policies_v1",
  "hazard_mitigation_assistance_mitigated_properties_v1",
  "hazard_mitigation_assistance_mitigated_properties_v2",
  "hazard_mitigation_assistance_projects_by_nfip_crs_communities_v1",
  "hazard_mitigation_assistance_projects_v1",
  "hazard_mitigation_assistance_projects_v2",
  "hazard_mitigation_grant_program_disaster_summaries_v1",
  "hazard_mitigation_grant_program_property_acquisitions_v1",
  "hazard_mitigation_grants_v1",
  "hazard_mitigation_plan_statuses_v1",
  "housing_assistance_owners_v1",
  "housing_assistance_owners_v2",
  "housing_assistance_renters_v1",
  "housing_assistance_renters_v2",
  "individual_assistance_housing_registrants_large_disasters_v1",
  "individuals_and_households_program_valid_registrations_v1",
  "ipaws_archived_alerts_v1",
  "mission_assignments_v1",
  "non_disaster_assistance_firefighter_grants_v1",
  "public_assistance_applicants_v1",
  "public_assistance_funded_projects_details_v1",
  "public_assistance_funded_projects_summaries_v1",
  "registration_intake_individuals_household_programs_v1",
  "registration_intake_individuals_household_programs_v2"
]

const type_map = {
  number: "numeric",
  string: "text",
  date: "timestamp with time zone",
  boolean: "boolean",
}

const createSchema = async (sqlLog, ctx) => {
  // create schema
  const createSchema = `CREATE SCHEMA IF NOT EXISTS open_fema_data;`;
  sqlLog.push(createSchema);
  await ctx.call("dama_db.query", {
    text: createSchema
  });
}

const upload = async (ctx, view_id, table_name, dbConnection) => {
  return fetch(url)
    .then(res => res.json())
    .then(data => {
      let metadata = data.OpenFemaDataSets
      let current_total = 0
      const newData = metadata.reduce((out, curr) => {
        out[camelToSnakeCase(curr.name).substr(1) + '_v' + curr.version] = Object.keys(curr).reduce((snake, col) => {
          snake[camelToSnakeCase(col)] = curr[col]
          return snake
        }, {})
        out[camelToSnakeCase(curr.name).substr(1) + '_v' + curr.version].metadata_url = `https://www.fema.gov/api/open/v1/OpenFemaDataSetFields?$filter=openFemaDataSet%20eq%20%27${curr.name}%27%20and%20datasetVersion%20eq%20${curr.version}`
        return out
      }, {})

      let inserts = datasets
        .filter(key => key === table_name)
        .map(key => {
        const {
          title,
          description,
          web_service,
          data_dictionary,
          landing_page,
          publisher,
          last_refresh,
          metadata_url,
          record_count
        } = newData[key]

        return {
          title,
          description: description.split('\n').join(''),
          table: `open_fema_data.${key}`,
          data_url: web_service,
          data_dictionary,
          landing_page,
          publisher,
          last_refresh,
          record_count

        }
      })

      return Promise.all(
        datasets
          .filter(k => k === table_name)
          .map(k => {
          return fetch(newData[k].metadata_url)
            .then(res => res.json())
            .then(dict => {
              return {
                name: k,
                schema: 'open_fema_data',
                columns: dict.OpenFemaDataSetFields.map(d => {
                  return {
                    name: camelToSnakeCase(d.name),
                    dataType: type_map[d.type.trim()] || d.type.trim(),
                    primaryKey: d.primaryKey ? true : false
                  };
                }),
              };
            });
        })
      ).then(async (values) => {
        return values.reduce(async (out, table) => {
          await out;

          const createSql = sql.define(tables[table.name](view_id)).create().ifNotExists().toQuery();

          await ctx.call("dama_db.query", {text: createSql.text});

          table.name = `${table.name}_${view_id}`;
          out[table.name] = table;

          return updateChunks(inserts[0], ctx, table.columns, view_id, dbConnection);
        }, {});
      });
    });
};

const updateChunks = async (source, ctx, cols, view_id, dbConnection) => {
  let skips = []
  let progress = 0

  const [schema,table] = source.table.split('.');
  const sql_table = sql.define(tables[table](view_id));
  //   const sql_table = sql.define(Object.assign({}, tables[table](view_id), {columns: cols}));
  console.log('total', source.record_count, 'records')
  for(let i=0; i < source.record_count; i+=1000){
    skips.push(i)
  }
  console.log('skips', skips)
  return Promise.map(skips,(skip =>{
    return new Promise((resolve,reject) => {
      console.time(`fetch skip ${skip}`);
      console.log(`${source.data_url}?$skip=${skip}`)
      fetch(`${source.data_url}?$skip=${skip}`)
        .then(res => res.json())
        .then(res => {
          console.timeEnd(`fetch skip ${skip}`);
          let dataKey = source.data_url.split('/')[source.data_url.split('/').length-1]
          let data = res[dataKey]
          let notNullCols = [
            ...tables[table](view_id).columns.filter(c => c.dataType === 'numeric').map(c => c.name),
            'project_size' // PA specific
          ]
          const newData = data.map((curr) => {
            return Object.keys(curr)
              .filter(col => sql_table.columns.map(c => c.name).includes(camelToSnakeCase(col)))
              .reduce((snake, col) => {
              if(notNullCols.includes(camelToSnakeCase(col)) && !curr[col]){
                snake[camelToSnakeCase(col)] = 0; // nulls in numeric
              }else{
                snake[camelToSnakeCase(col)] = curr[col]
              }
              return snake
            },{})

          },{})

          Promise.all(
            chunk(newData,500)
              //.filter((k,i) => i < 1)
              .map(chunk =>{
                let query = sql_table
                  .insert(Object.values(chunk)) // upsert datasources
                  .onConflict({
                    columns: tables[table](view_id).columns.filter(k => k.primaryKey).map(d => d.name),
                    update: tables[table](view_id).columns.filter(k => !k.primaryKey).map(d => d.name)
                  })
                  .toQuery()

                return ctx.call("dama_db.query", query).then(() => dbConnection.query("COMMIT;"));
              })
          )
            .then(ins => {
              let rows = ins.reduce((a,b)=> {
                if(b.rowCount && !isNaN(+b.rowCount)){
                  a += b.rowCount
                }
                return a
              },0)
              progress += rows
              console.log(progress, ((progress / source.record_count) * 100 ).toFixed(1), '%')
              resolve(rows)
            })

        })
    })
  }),{concurrency: 3})
}

export default async function publish(ctx: Context) {
  let {
    // @ts-ignore
    params: { table_name, source_name, version, existing_source_id },
  } = ctx;

  const {etl_context_id, dbConnection, sqlLog} = await init(ctx);

  const {source_id} = existing_source_id ? existing_source_id : await ctx.call("dama/metadata.createNewDamaSource", {name: source_name, type: table_name});

  const {view_id} = await ctx.call("dama/metadata.createNewDamaView", {source_id, version});

  try {

    await createSchema(sqlLog, ctx);
    await upload(ctx, view_id, table_name, dbConnection);
    await update_view({table_schema: 'open_fema_data', table_name, view_id, dbConnection, sqlLog});

    return fin({etl_context_id, ctx, dbConnection, payload: {view_id, source_id}});
  } catch (e) {
    return err({e, etl_context_id, sqlLog, ctx, dbConnection});
  }
}
