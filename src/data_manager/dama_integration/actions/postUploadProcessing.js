const updateCoords = async (ctx, details_table_name) => {
    let query = `
                update severe_weather_new.${details_table_name} dst
                set begin_coords_geom = st_setsrid(st_point(src.begin_lon, src.begin_lat), 4326),
                    end_coords_geom   = st_setsrid(st_point(src.end_lon, src.end_lat), 4326)
                from severe_weather_new.${details_table_name} src
                where src.event_id = dst.event_id
                  and src.episode_id = dst.episode_id
                and src.begin_lon != 0
                and src.begin_lat != 0
                and src.end_lon != 0
                and src.end_lat != 0;
`
    return ctx.call("dama_db.query", {
      text: query,
    });
}

const updateDamage = async (ctx, details_table_name) => {
    let query = `
        update severe_weather_new.${details_table_name}
        set property_damage =
            CASE
                WHEN char_length(damage_property) > 1
                THEN
                    CASE
                          WHEN RIGHT(damage_property,1) = 'B' THEN cast(cast(LEFT(damage_property,-1) as float) * 1000000000 as bigint)
                          WHEN RIGHT(damage_property,1) = 'M' THEN cast(cast(LEFT(damage_property,-1) as float) * 1000000 as bigint)
                          WHEN RIGHT(damage_property,1) = 'K' THEN cast(cast(LEFT(damage_property,-1) as float) * 1000 as bigint)
                          WHEN RIGHT(damage_property,1) = 'H' or RIGHT(damage_property,1) = 'h' THEN cast(cast(LEFT(damage_property,-1) as float) * 100 as bigint)
                          ELSE cast(cast(damage_property as float) as bigint)
                    END
            END,
            crop_damage =
            CASE
                WHEN char_length(damage_crops) > 1
                THEN
                    CASE
                          WHEN RIGHT(damage_crops,1) = 'B' THEN cast(cast(LEFT(damage_crops,-1) as float) * 1000000000 as bigint)
                          WHEN RIGHT(damage_crops,1) = 'M' THEN cast(cast(LEFT(damage_crops,-1) as float) * 1000000 as bigint)
                          WHEN RIGHT(damage_crops,1) = 'K' or RIGHT(damage_crops,1) = 'k' or RIGHT(damage_crops,1) = 'T' THEN cast(cast(LEFT(damage_crops,-1) as float) * 1000 as bigint)
                          WHEN RIGHT(damage_crops,1) = 'H' or RIGHT(damage_crops,1) = 'h' THEN cast(cast(LEFT(damage_crops,-1) as float) * 100 as bigint)
                          WHEN RIGHT(damage_crops,1) = '?' THEN 0
                          ELSE cast(cast(damage_crops as float) as bigint)
                    END
            END;
    `
  return ctx.call("dama_db.query", {
    text: query,
  });
}

const updateGeoTracts = async (ctx, details_table_name, tract_schema, tract_table) => { // geo.tl_2017_tract
    // some of the geoids from geo.tl_2017_tract mismatch severe_weather_new.details. priority given to geo.tl_2017_tract
    let query = `
        with t as (
            select b.geoid, geom
            from ${tract_schema}.${tract_table} b
        ),
             s as (
                 select event_id, begin_coords_geom
                 from severe_weather_new.${details_table_name}
                 where geoid is null
                   and begin_coords_geom is not null
             ),
             a as (
                 select s.event_id, t.geoid geoid
                 from s
                          join t
                               on st_contains(t.geom, begin_coords_geom)
             )

        update severe_weather_new.${details_table_name} dst
        set geoid = a.geoid from a
        where dst.event_id = a.event_id;


    `
  return ctx.call("dama_db.query", {
    text: query,
  });
}

const updateGeoCounties = async (ctx, details_table_name) => {
    let query = `
        UPDATE severe_weather_new.${details_table_name}
        SET geoid = LPAD(state_fips::TEXT, 2, '0') || LPAD(cz_fips::TEXT, 3, '0')
        WHERE geoid IS NULL;
    `
  return ctx.call("dama_db.query", {
    text: query,
  });
}

const updateGeoCousubs = async (ctx, details_table_name, cousub_schema, cousub_trable) => { // geo.tl_2017_cousub
    let query = `
        with s as (
                    SELECT event_id, a.geoid
                    FROM ${cousub_schema}.${cousub_trable} a
                        join severe_weather_new.${details_table_name} t
                            on st_contains(a.geom, t.begin_coords_geom)
                                   and t.cousub_geoid is null
                                   and t.begin_coords_geom is not null
                                   and substring(a.geoid::text, 1, 2) = substring(t.geoid::text, 1, 2)
                    )

        update severe_weather_new.${details_table_name} dst
        set cousub_geoid = s.geoid
        from s
        where dst.event_id = s.event_id;`;

  return ctx.call("dama_db.query", {
    text: query,
  });
}

const removeGeoidZzone = async (ctx, details_table_name) => {
    let query = `
    UPDATE severe_weather_new.${details_table_name} d
        SET geoid = null
        WHERE begin_lat = '0'
        AND cz_type = 'Z';
        `
  return ctx.call("dama_db.query", {
    text: query,
  });
}

const updateGeoZzone = async (ctx, details_table_name, ztc_schema, ztc_table) => { // severe_weather.zone_to_county
    // todo first set geoid to null for Z, where begin_coords = 0 (removeGeoidZzone), then run the following
    let query = `
        UPDATE severe_weather_new.${details_table_name} d
        SET geoid = lpad(fips::text, 5, '0')
        FROM ${ztc_schema}.${ztc_table} z
        WHERE z.zone = d.cz_fips
        AND begin_lat = '0'
        AND cz_type = 'Z';
    `
  return ctx.call("dama_db.query", {
    text: query,
  });
}

// const updateGeoZzoneV2 = async (ctx) => {
//     // -- zone M should be null
//     // -- records with begin_coords should map to geo.tl....
//     // -- records with zone Z and no begin_coords should follow below sql
//     // -- Z zone, no being_coords records with cz_name not mapping to county names remain null as there's no right way to map them
//     let query = `
//         with states as (
//             SELECT id, geoid, stusps, name
//             FROM geo.tl_2017_us_state
//         ),
//         zone_to_county as (
//             select zone, state, stusps, states.geoid, county, lpad(fips::text, 5, '0') fips
//             from severe_weather.zone_to_county
//             JOIN states
//             ON states.stusps = state
//             order by 1, 2, 3
//         )
//
//         -- select
//         -- cz_fips d_cz_fips, d.state d_state, state_fips d_state_fips, cz_name d_zone_name,
//         -- zone ztc_zone, ztc.state ztc_state, county ztc_county, lpad(fips::text, 5, '0') ztc_fips, d.geoid, d.tmp_geoid,
//         -- case when lpad(fips::text, 5, '0') = d.geoid then 1 else 0 end
//         -- from severe_weather_new.${details_table_name} d
//         -- JOIN zone_to_county ztc
//         -- on d.cz_fips = ztc.zone
//         -- AND LPAD(state_fips::TEXT, 2, '0') = ztc.geoid
//         -- AND lower(cz_name) like '%' || lower(county) || '%'
//         -- where cz_type = 'Z' and begin_lat = '0' and lpad(fips::text, 5, '0') != d.geoid
//         -- order by 1, 2
//         -- limit 1000
//
//         UPDATE severe_weather_new.${details_table_name} d
//         SET tmp_geoid = lpad(fips::text, 5, '0')
//         FROM zone_to_county ztc
//         WHERE (cz_type = 'Z' and begin_lat = '0')
//         AND d.cz_fips = ztc.zone
//         AND LPAD(state_fips::TEXT, 2, '0') = ztc.geoid
//         AND lower(cz_name) like '%' || lower(county) || '%'
//         `
//
//   return ctx.call("dama_db.query", {
//     text: query,
//   });
// }

const updateGeoMzone = async (ctx, details_table_name) => {
    let query = `
        update severe_weather_new.${details_table_name}
        set geoid = null
        where cz_type = 'M';
    `
  return ctx.call("dama_db.query", {
    text: query,
  });
}

const updateDateTime = async (ctx, details_table_name) => {
    let query = `
        UPDATE severe_weather_new.${details_table_name}
        SET begin_date_time =
                (LEFT(begin_yearmonth::text, 4) || '-' || RIGHT(begin_yearmonth::text, 2) || '-' || begin_day::text ||
                 ' ' ||
                 LPAD(LEFT(begin_time::text, LENGTH(begin_time::text) - 2), 2, '0') || ':' || RIGHT(begin_time::text, 2)
                    )::TIMESTAMP,
            end_date_time   =
                (LEFT(end_yearmonth::text, 4) || '-' || RIGHT(end_yearmonth::text, 2) || '-' || end_day::text || ' ' ||
                 LPAD(LEFT(end_time::text, LENGTH(end_time::text) - 2), 2, '0') || ':' || RIGHT(end_time::text, 2)
                    )::TIMESTAMP
        where extract(YEAR from begin_date_time) > 2021;
    `
  return ctx.call("dama_db.query", {
    text: query,
  });
}

const createIndices = async (ctx, details_table_name) => {
    let query = `
      BEGIN;
      CREATE INDEX IF NOT EXISTS begin_coords_geom_idx_${details_table_name}
        ON severe_weather_new.${details_table_name} USING btree
        (begin_coords_geom ASC NULLS LAST)
        TABLESPACE pg_default;
      -- Index: begin_date_time_idx

-- DROP INDEX IF EXISTS severe_weather_new.begin_date_time_idx;

      CREATE INDEX IF NOT EXISTS begin_date_time_idx_${details_table_name}
        ON severe_weather_new.${details_table_name} USING btree
        (begin_date_time ASC NULLS LAST)
        TABLESPACE pg_default;
      -- Index: details_cousub_geoid_idx

-- DROP INDEX IF EXISTS severe_weather_new.${details_table_name}_cousub_geoid_idx;

      CREATE INDEX IF NOT EXISTS details_cousub_geoid_idx_${details_table_name}
        ON severe_weather_new.${details_table_name} USING btree
        (cousub_geoid COLLATE pg_catalog."default" ASC NULLS LAST)
        TABLESPACE pg_default;
      -- Index: details_event_type_idx

-- DROP INDEX IF EXISTS severe_weather_new.${details_table_name}_event_type_idx;

      CREATE INDEX IF NOT EXISTS details_event_type_idx_${details_table_name}
        ON severe_weather_new.${details_table_name} USING btree
        (event_type COLLATE pg_catalog."default" ASC NULLS LAST)
        TABLESPACE pg_default;
      -- Index: details_geoid2_idx

-- DROP INDEX IF EXISTS severe_weather_new.${details_table_name}_geoid2_idx;

      CREATE INDEX IF NOT EXISTS details_geoid2_idx_${details_table_name}
        ON severe_weather_new.${details_table_name} USING btree
        ("substring"(geoid::text, 1, 2) COLLATE pg_catalog."default" ASC NULLS LAST)
        TABLESPACE pg_default;
      -- Index: details_geoid5_idx

-- DROP INDEX IF EXISTS severe_weather_new.${details_table_name}_geoid5_idx;

      CREATE INDEX IF NOT EXISTS details_geoid5_idx_${details_table_name}
        ON severe_weather_new.${details_table_name} USING btree
        ("substring"(geoid::text, 1, 5) COLLATE pg_catalog."default" ASC NULLS LAST)
        TABLESPACE pg_default;
      -- Index: details_geoid_idx

-- DROP INDEX IF EXISTS severe_weather_new.${details_table_name}_geoid_idx;

      CREATE INDEX IF NOT EXISTS details_geoid_idx_${details_table_name}
        ON severe_weather_new.${details_table_name} USING btree
        (geoid COLLATE pg_catalog."default" ASC NULLS LAST)
        TABLESPACE pg_default;
      -- Index: details_property_damage_idx

-- DROP INDEX IF EXISTS severe_weather_new.${details_table_name}_property_damage_idx;

      CREATE INDEX IF NOT EXISTS details_property_damage_idx_${details_table_name}
        ON severe_weather_new.${details_table_name} USING btree
        (property_damage ASC NULLS LAST)
        TABLESPACE pg_default;
      -- Index: end_date_time

-- DROP INDEX IF EXISTS severe_weather_new.end_date_time;

      CREATE INDEX IF NOT EXISTS end_date_time_${details_table_name}
        ON severe_weather_new.${details_table_name} USING btree
        (end_date_time ASC NULLS LAST)
        TABLESPACE pg_default;
      -- Index: event_id_idx

-- DROP INDEX IF EXISTS severe_weather_new.event_id_idx;

      CREATE UNIQUE INDEX IF NOT EXISTS event_id_idx_${details_table_name}
        ON severe_weather_new.${details_table_name} USING btree
        (event_id ASC NULLS LAST)
        INCLUDE(event_id)
        TABLESPACE pg_default;
      -- Index: event_type_idx

-- DROP INDEX IF EXISTS severe_weather_new.event_type_idx;

      CREATE INDEX IF NOT EXISTS event_type_idx_${details_table_name}
        ON severe_weather_new.${details_table_name} USING btree
        (event_type COLLATE pg_catalog."default" ASC NULLS LAST)
        TABLESPACE pg_default;
    COMMIT;
    `
  return ctx.call("dama_db.query", {
    text: query,
  });
}

export const postProcess = async (ctx, details_table_name, tract_schema, tract_table, cousub_schema, cousub_table, ztc_schema, ztc_table) => {
  console.log("Welcome to post upload processing...");
  // await createIndices(ctx, details_table_name);
  // await updateCoords(ctx, details_table_name);
  // await updateDamage(ctx, details_table_name);
  await updateGeoTracts(ctx, details_table_name, tract_schema, tract_table);
  console.log('1')
  await updateGeoCounties(ctx, details_table_name);
  console.log('2')
  await updateGeoCousubs(ctx, details_table_name, cousub_schema, cousub_table); // uses geoid
  console.log('3')
  await removeGeoidZzone(ctx, details_table_name);
  await updateGeoZzone(ctx, details_table_name, ztc_schema, ztc_table);
  // await updateGeoZzoneV2(ctx);
  await updateGeoMzone(ctx, details_table_name);
  await updateDateTime(ctx, details_table_name);

  return Promise.resolve();
}
