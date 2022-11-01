const updateCoords = async (ctx) => {
    let query = `
                update severe_weather_new.details dst
                set begin_coords_geom = st_setsrid(st_point(src.begin_lon, src.begin_lat), 4326),
                    end_coords_geom   = st_setsrid(st_point(src.end_lon, src.end_lat), 4326)
                from severe_weather_new.details src
                where src.event_id = dst.event_id
                  and src.episode_id = dst.episode_id
                and src.begin_lon != 0
                and src.begin_lat != 0
                and src.end_lon != 0
                and src.end_lat != 0
`
    return ctx.call("dama_db.query", {
      text: query,
    });
}

const updateDamage = async (ctx) => {
    let query = `
        update severe_weather_new.details
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
            END
    `
  return ctx.call("dama_db.query", {
    text: query,
  });
}

const updateGeoTracts = async (ctx) => {
    // some of the geoids from geo.tl_2017_tract mismatch severe_weather_new.details. priority given to geo.tl_2017_tract
    let query = `
        with t as (
            select b.geoid, st_setsrid(geom, 4326) geom
            from geo.tl_2017_tract b
        ),
             s as (
                 select event_id, begin_coords_geom
                 from severe_weather_new.details
                 where geoid is null
                   and begin_coords_geom is not null
             ),
             a as (
                 select s.event_id, begin_coords_geom, t.geoid geoid
                 from s
                          join t
                               on st_contains(t.geom, begin_coords_geom)
             )

        update severe_weather_new.details dst
        set geoid = a.geoid from a
        where dst.event_id = a.event_id


    `
  return ctx.call("dama_db.query", {
    text: query,
  });
}

const updateGeoCounties = async (ctx) => {
    let query = `
        UPDATE severe_weather_new.details
        SET geoid = LPAD(state_fips::TEXT, 2, '0') || LPAD(cz_fips::TEXT, 3, '0')
        WHERE geoid IS NULL
    `
  return ctx.call("dama_db.query", {
    text: query,
  });
}

const updateGeoCousubs = async (ctx) => {
    let query = `
        with t as (
            select event_id, st_transform(begin_coords_geom, 4269) geom
            from severe_weather_new.details
            where cousub_geoid is null
              and begin_coords_geom is not null
        ),
             s as (
                 SELECT event_id, geoid
                 FROM geo.tl_2017_cousub a
                          join t
                               on st_contains(a.geom, t.geom)
             )

        update severe_weather_new.details dst
        set cousub_geoid = s.geoid from s
        where dst.event_id = s.event_id
    `
  return ctx.call("dama_db.query", {
    text: query,
  });
}

const removeGeoidZzone = async (ctx) => {
    let query = `
    UPDATE severe_weather_new.details d
        SET geoid = null
        WHERE begin_lat = '0'
        AND cz_type = 'Z'
        `
  return ctx.call("dama_db.query", {
    text: query,
  });
}

const updateGeoZzone = async (ctx) => {
    // todo first set geoid to null for Z, where begin_coords = 0 (removeGeoidZzone), then run the following
    let query = `
        UPDATE severe_weather_new.details d
        SET geoid = lpad(fips::text, 5, '0')
        FROM severe_weather.zone_to_county z
        WHERE z.zone = d.cz_fips
        AND begin_lat = '0'
        AND cz_type = 'Z'
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
//         -- from severe_weather_new.details d
//         -- JOIN zone_to_county ztc
//         -- on d.cz_fips = ztc.zone
//         -- AND LPAD(state_fips::TEXT, 2, '0') = ztc.geoid
//         -- AND lower(cz_name) like '%' || lower(county) || '%'
//         -- where cz_type = 'Z' and begin_lat = '0' and lpad(fips::text, 5, '0') != d.geoid
//         -- order by 1, 2
//         -- limit 1000
//
//         UPDATE severe_weather_new.details d
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

const updateGeoMzone = async (ctx) => {
    let query = `
        update severe_weather_new.details
        set geoid = null
        where cz_type = 'M'
    `
  return ctx.call("dama_db.query", {
    text: query,
  });
}

const updateDateTime = async (ctx) => {
    let query = `
        UPDATE severe_weather_new.details
        SET begin_date_time =
                (LEFT(begin_yearmonth::text, 4) || '-' || RIGHT(begin_yearmonth::text, 2) || '-' || begin_day::text ||
                 ' ' ||
                 LPAD(LEFT(begin_time::text, LENGTH(begin_time::text) - 2), 2, '0') || ':' || RIGHT(begin_time::text, 2)
                    )::TIMESTAMP,
            end_date_time   =
                (LEFT(end_yearmonth::text, 4) || '-' || RIGHT(end_yearmonth::text, 2) || '-' || end_day::text || ' ' ||
                 LPAD(LEFT(end_time::text, LENGTH(end_time::text) - 2), 2, '0') || ':' || RIGHT(end_time::text, 2)
                    )::TIMESTAMP
        where extract(YEAR from begin_date_time) > 2021
    `
  return ctx.call("dama_db.query", {
    text: query,
  });
}

export const postProcess = async (ctx) => {
  console.log("Welcome to post upload processing...");
    await updateCoords(ctx);
    await updateDamage(ctx);
    await updateGeoTracts(ctx);
    await updateGeoCounties(ctx);
    await updateGeoCousubs(ctx);
    await removeGeoidZzone(ctx);
    await updateGeoZzone(ctx);
    // await updateGeoZzoneV2(ctx);
    await updateGeoMzone(ctx);
    await updateDateTime(ctx);
}
