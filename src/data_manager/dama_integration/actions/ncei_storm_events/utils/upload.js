import fs from "fs";
import _ from "lodash";
import sql from "sql";
import {tables} from "./tables";

sql.setDialect("postgres");

export const loadFiles = (table, dama_view_id, ctx) => {
  const working_table = Object.assign({}, tables[table], {name: `${tables[table].name}${dama_view_id ? `_${dama_view_id}` : ``}`})
  console.log('welcome to upload!', working_table)
  const details = sql.define(working_table);
  const pathToFiles = "data/";
  console.log("uploading", table);
    const files = fs.readdirSync(pathToFiles + table).filter(f => f.substr(0, 1) !== "."); // filtering any open files

    return files
        .reduce(async (acc, file, fileI) => {
            // if(fileI < 47) return Promise.resolve();
            await acc;
            return new Promise((resolve, reject) => {
                console.log(`file ${++fileI} of ${files.length} ${fileI*100/files.length}% ${file}`);
                console.log(pathToFiles + table + "/" + file)
                fs.readFile(pathToFiles + table + "/" + file, "utf8", (err,d) => {

                    const headers = d.split(/\r?\n/).slice(0, 1)[0].split("\t").map(h => h.toLowerCase());

                    const values =
                        d.split(/\r?\n/)
                            .slice(1, d.split(/\r?\n/).length)
                            .map(d1 => d1.split("\t"))
                            .filter(d2 => d2.length > 1)
                            .map((d2) => {
                                return d2.reduce((acc_d2, value, index) => {
                                  if(acc_d2.length > headers.length){
                                    console.log('checking len', headers, acc_d2)
                                  }
                                  if(!headers[index]){
                                    console.log('erroring here', index, headers.length)
                                    console.log(d2)
                                  }
                                    acc_d2[headers[index]] =
                                            (working_table.numericColumns || []).includes(headers[index]) && [null, "", " ", undefined].includes(value) ?
                                                0 :
                                                (working_table.dateColumns || []).includes(headers[index]) && [0, "0"].includes(value) ?
                                                    null :
                                                    (working_table.numericColumns || []).includes(headers[index]) && typeof value !== "number" && value ?
                                                        parseInt(value) :
                                                            (working_table.floatColumns || []).includes(headers[index]) && typeof value !== "number" && value ?
                                                                parseFloat(value) :
                                                                    value;
                                    return acc_d2;
                                }, {});
                            });

                    resolve(_.chunk(values, 500)
                            .reduce(async (accChunk, chunk, chunkI) => {
                                await accChunk;
                                const query = details.insert(chunk).toQuery();
                                // return db.query(query.text,query.values)

                              return  ctx.call("dama_db.query", {
                                text: query.text,
                                values: query.values,
                              });
                            }, Promise.resolve()));
                });
            });
    }, Promise.resolve());

};

export const createSqls = (table_name, viewId, schema_name) => {
  const sqls = {
    details: `
              CREATE TABLE IF NOT EXISTS ${schema_name || tables.details.schema}.${table_name || tables.details.name}${viewId ? `_${viewId}` : ``}
              (
                  begin_yearmonth integer,
                  begin_day integer,
                  begin_time integer,
                  end_yearmonth integer,
                  end_day integer,
                  end_time integer,
                  episode_id integer,
                  event_id integer NOT NULL,
                  state character varying COLLATE pg_catalog."default",
                  state_fips integer,
                  year integer,
                  month_name character varying COLLATE pg_catalog."default",
                  event_type character varying COLLATE pg_catalog."default",
                  cz_type character varying COLLATE pg_catalog."default",
                  cz_fips integer,
                  cz_name character varying COLLATE pg_catalog."default",
                  wfo character varying COLLATE pg_catalog."default",
                  begin_date_time timestamp without time zone,
                  cz_timezone character varying COLLATE pg_catalog."default",
                  end_date_time timestamp without time zone,
                  injuries_direct integer,
                  injuries_indirect integer,
                  deaths_direct integer,
                  deaths_indirect integer,
                  damage_property character varying COLLATE pg_catalog."default",
                  damage_crops character varying COLLATE pg_catalog."default",
                  source character varying COLLATE pg_catalog."default",
                  magnitude double precision,
                  magnitude_type character varying COLLATE pg_catalog."default",
                  flood_cause character varying COLLATE pg_catalog."default",
                  category character varying COLLATE pg_catalog."default",
                  tor_f_scale character varying COLLATE pg_catalog."default",
                  tor_length double precision,
                  tor_width double precision,
                  tor_other_wfo character varying COLLATE pg_catalog."default",
                  tor_other_cz_state character varying COLLATE pg_catalog."default",
                  tor_other_cz_fips character varying COLLATE pg_catalog."default",
                  tor_other_cz_name character varying COLLATE pg_catalog."default",
                  begin_range integer,
                  begin_azimuth character varying COLLATE pg_catalog."default",
                  begin_location character varying COLLATE pg_catalog."default",
                  end_range integer,
                  end_azimuth character varying COLLATE pg_catalog."default",
                  end_location character varying COLLATE pg_catalog."default",
                  begin_lat double precision,
                  begin_lon double precision,
                  end_lat double precision,
                  end_lon double precision,
                  episode_narrative character varying COLLATE pg_catalog."default",
                  event_narrative character varying COLLATE pg_catalog."default",
                  data_source character varying COLLATE pg_catalog."default",
                  begin_coords_geom geometry(Point,4326),
                  end_coords_geom geometry(Point,4326),
                  property_damage bigint DEFAULT 0,
                  crop_damage bigint DEFAULT 0,
                  geoid character varying(11) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
                  cousub_geoid character varying(10) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
                  event_type_formatted character varying COLLATE pg_catalog."default",
                  nri_category character varying COLLATE pg_catalog."default",
                  property_damage_adjusted double precision,
                  crop_damage_adjusted double precision,
                  CONSTRAINT event_id_pkey${viewId ? `_${viewId}` : ``} PRIMARY KEY (event_id)
                      INCLUDE(event_id)
              )`,
    // tl_2017_cousub: `
    //           CREATE TABLE IF NOT EXISTS ${schema_name || tables.tl_2017_cousub.schema}.${table_name || tables.tl_2017_cousub.name}${viewId ? `_${viewId}` : ``}
    //           (
    //               geom geometry(MultiPolygon,4326),
    //               statefp character varying(2) COLLATE pg_catalog."default",
    //               countyfp character varying(3) COLLATE pg_catalog."default",
    //               geoid character varying(10) COLLATE pg_catalog."default",
    //               name character varying(100) COLLATE pg_catalog."default",
    //               namelsad character varying(100) COLLATE pg_catalog."default"
    //           )
    // `,
    zone_to_county: `
              CREATE TABLE IF NOT EXISTS ${schema_name || tables.zone_to_county.schema}.${table_name || tables.zone_to_county.name}${viewId ? `_${viewId}` : ``}
              (
                  "id" integer,
                  geom geometry(Point,4326),
                  state character varying COLLATE pg_catalog."default",
                  zone integer,
                  cwa character varying COLLATE pg_catalog."default",
                  name character varying COLLATE pg_catalog."default",
                  state_zone character varying COLLATE pg_catalog."default",
                  county character varying COLLATE pg_catalog."default",
                  fips integer,
                  time_zone character varying COLLATE pg_catalog."default",
                  fe_area character varying COLLATE pg_catalog."default",
                  lat double precision,
                  lon double precision,
                  CONSTRAINT zone_to_county_pkey${viewId ? `_${viewId}` : ``} PRIMARY KEY (id)
              )
    `
  }

  return sqls[table_name];
}
