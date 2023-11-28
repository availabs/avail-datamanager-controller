#!/usr/bin/env node

/*
ogr2ogr -F 'OpenFileGDB' s_fp_pct.gdb s_fp_pct.gpkg
ogrinfo s_fp_pct.gdb/ -sql "ALTER TABLE S_Fld_Haz_Ar RENAME COLUMN SHAPE_Length TO SHAPE_Leng"
*/

require("ts-node").register();
require("tsconfig-paths").register();

const dama_db = require("../../../../data_manager/dama_db").default;

const pg_env = "dama_dev_1";

const footprint_floodzone_overlaps_table_name =
  "building_footprint_floodzone_overlaps";

const floodzone_table_names = [
  "overlapped_nhfl",
  "overlapped_ble",
  "overlapped_q3",
  "nonoverlapped_preliminary",
  "nonoverlapped_nhfl",
  "nonoverlapped_ble",
  "nonoverlapped_q3",
];

const source_table_meta = {
  preliminary: {
    source_table_name: "merged_preliminary",
    source_id_col: "fid",
  },
  nhfl: {
    source_table_name: "nhfl",
    source_id_col: "ogc_fid",
  },
  ble: {
    source_table_name: "merged_ble",
    source_id_col: "objectid",
  },
  q3: {
    source_table_name: "merged_q3",
    source_id_col: "fid",
  },
};

async function createFootprintFloodzoneOverlapsTable() {
  const sql = `
    BEGIN ;

    DROP TABLE IF EXISTS floodplains.${footprint_floodzone_overlaps_table_name} CASCADE;
      
    CREATE TABLE IF NOT EXISTS floodplains.${footprint_floodzone_overlaps_table_name} (
      building_id                           BIGINT,

      floodzone_map                         TEXT NOT NULL,
      floodzone_id                          INTEGER NOT NULL,

      e_bld_rsk_flood_zone                  TEXT,

      map_flood_zone                        TEXT,
      map_zone_subty                        TEXT,

      PRIMARY KEY (building_id, floodzone_map, floodzone_id)
    ) ;

    COMMIT ;
  `;

  await dama_db.query(sql, pg_env);
}

async function findOverlaps() {
  for (const floodzone_map of floodzone_table_names) {
    const type = floodzone_map.replace(/.*_/, "");
    const { source_table_name, source_id_col } = source_table_meta[type];

    console.log("INSERT buildings overlapping ", floodzone_map);
    console.time(floodzone_map);

    const div_geom_table_name = `${floodzone_map}_divided_geom`;

    const sql = `
      INSERT INTO floodplains.${footprint_floodzone_overlaps_table_name} (
        building_id,

        floodzone_map,
        floodzone_id,

        e_bld_rsk_flood_zone,

        map_flood_zone,
        map_zone_subty
      )
        SELECT DISTINCT
            a.building_id,

            '${floodzone_map}' AS floodzone_map,
            b.id AS floodzone_id,

            a.flood_zone  AS e_bld_rsk_flood_zone,

            c.fld_zone    AS map_flood_zone,
            c.zone_subty  AS map_zone_subty

          FROM floodplains.enhanced_building_risk_geom AS a
            INNER JOIN floodplains.${div_geom_table_name} AS b
              ON ( ST_Intersects(a.geom, b.wkb_geometry) )
            INNER JOIN floodplains.${source_table_name} AS c
              ON ( b.id = c.${source_id_col} )
    `;

    await dama_db.query(sql, pg_env);
    console.timeEnd(floodzone_map);
  }
}

async function createInterestingBuildingsView() {
  const table_name = `interesting_${footprint_floodzone_overlaps_table_name}`;
  const create_table_sql = `
    DROP TABLE IF EXISTS floodplains.${table_name} ;

    CREATE TABLE floodplains.interesting_${footprint_floodzone_overlaps_table_name} (
      LIKE floodplains.${footprint_floodzone_overlaps_table_name} INCLUDING ALL,

      wkb_geometry public.geometry(MultiPolygon, 4326)
    ) ;
  `;

  await dama_db.query(create_table_sql, pg_env);

  const nonoverlap_floodzone_table_names = floodzone_table_names.filter((t) =>
    /^nonoverlapped/.test(t)
  );

  for (const floodzone_map of nonoverlap_floodzone_table_names) {
    const type = floodzone_map.replace(/.*_/, "");
    console.log("INSERT interesting", type);

    const sql = `
      INSERT INTO floodplains.${table_name} (
        building_id,
        floodzone_map,
        floodzone_id,
        e_bld_rsk_flood_zone,
        map_flood_zone,
        map_zone_subty,
        wkb_geometry
      )
        SELECT DISTINCT
            a.building_id,
            a.floodzone_map,
            a.floodzone_id,
            a.e_bld_rsk_flood_zone,
            a.map_flood_zone,
            a.map_zone_subty,
            ST_Multi(c.geom) AS wkb_geometry
          FROM floodplains.${footprint_floodzone_overlaps_table_name} AS a
            INNER JOIN floodplains.interesting_${type} AS b
              ON (
                ( a.floodzone_id = b.id )
                AND
                ( floodzone_map = '${floodzone_map}' )
              )
          INNER JOIN floodplains.enhanced_building_risk_geom AS c
            ON ( a.building_id = c.building_id )
    `;

    await dama_db.query(sql, pg_env);
  }
}

async function main() {
  // await createFootprintFloodzoneOverlapsTable();
  // await findOverlaps();
  createInterestingBuildingsView();
}

main();
