import dedent from "dedent";
import pgFormat from "pg-format";

import dama_db from "data_manager/dama_db";

async function getRisMapVersion(conflation_map_version: string) {
  const text = dedent(`
    SELECT
        ris_map_version
      FROM conflation.conflation_map_ris_version
      WHERE ( conflation_map_version = $1 )
    ;
  `);

  const { rows } = await dama_db.query({
    text,
    values: [conflation_map_version],
  });

  if (rows.length === 0) {
    throw new Error(
      `No ris_map_version found for conflation_map_version ${conflation_map_version}`
    );
  }

  const [{ ris_map_version }] = rows;

  return ris_map_version;
}

async function getCreateViewDDL(conflation_version: string, year: number) {
  const normalized_conflation_map_version = conflation_version.replace(
    /\./g,
    "_"
  );

  const conflation_map_version = `${year}_v${normalized_conflation_map_version}`;

  const ris_map_version = await getRisMapVersion(conflation_map_version);

  const conflation_map_table_name = `conflation_map_${conflation_map_version}`;

  const tmc_metadata_table_name = `tmc_metadata_${year}`;

  const ris_map_table_name = `nys_roadway_inventory_system_v${ris_map_version}_gdb`;

  const mview_name = `${conflation_map_table_name}_ris_based_metadata`;

  const mview_idx_name = `${mview_name}_pkey`;

  const sql = dedent(
    pgFormat(
      `
        DROP MATERIALIZED VIEW IF EXISTS conflation.%I ;

        CREATE MATERIALIZED VIEW conflation.%I
          AS
            SELECT
                tmc,
                ROUND(
                  (
                    SUM( aadt_ris * conflmap_len_ft )
                    /
                    SUM( conflmap_len_ft * NULLIF(aadt_ris::BOOLEAN::INT, 0) )
                  )::DOUBLE PRECISION
                ) AS aadt_ris,
                ROUND(
                  (
                    SUM( aadt_singl_ris * conflmap_len_ft )
                    /
                    SUM( conflmap_len_ft * NULLIF(aadt_singl_ris::BOOLEAN::INT, 0) )
                  )::DOUBLE PRECISION
                ) AS aadt_singl_ris,
                ROUND(
                  (
                    SUM( aadt_combi_ris * conflmap_len_ft )
                    /
                    SUM( conflmap_len_ft * NULLIF(aadt_combi_ris::BOOLEAN::INT, 0) )
                  )::DOUBLE PRECISION
                ) AS aadt_combi_ris,
                ROUND(
                  (
                    SUM( posted_speed_limit * conflmap_len_ft )
                    /
                    SUM( conflmap_len_ft * NULLIF(posted_speed_limit::BOOLEAN::INT, 0) )
                  )::DOUBLE PRECISION
                ) AS avg_speedlimit_ris
              FROM (
                SELECT
                    b.tmc,                                  -- 1
                    b.miles * 5280 AS npmrds_len_ft,        -- 2
                    a.ris,                                  -- 3
                    c.aadt_current_yr_est AS aadt_ris,      -- 4
                    c.aadt_single_unit AS aadt_singl_ris,   -- 5
                    c.aadt_combo AS aadt_combi_ris,         -- 6
                    c.posted_speed_limit,                   -- 7
                    SUM (
                      ST_LENGTH(
                        GEOGRAPHY(a.wkb_geometry)
                      ) * 3.28084
                    ) AS conflmap_len_ft
                  FROM conflation.%I AS a
                    INNER JOIN public.%I AS b
                      USING (tmc)
                    INNER JOIN nysdot_ris.%I AS c
                      ON (
                        ( split_part(ris, ':', 1)::INTEGER = c.gis_id )
                        AND
                        ( split_part(ris, ':', 2)::DOUBLE PRECISION = c.beg_mp )
                      )
                  GROUP BY 1,2,3,4,5,6,7
              ) AS b
              GROUP BY tmc
        ;

        CREATE UNIQUE INDEX %I ON conflation.%I (tmc) ;

        CLUSTER conflation.%I USING %I ;

      `,
      // DROP
      mview_name,

      // CREATE
      mview_name,
      conflation_map_table_name,
      tmc_metadata_table_name,
      ris_map_table_name,

      // INDEX
      mview_idx_name,
      mview_name,

      // CLUSTER
      mview_name,
      mview_idx_name
    )
  );

  return sql;
}

export default async function main({ pg_env, conflation_version, year }) {
  await dama_db.runInTransactionContext(async () => {
    const ddl = await getCreateViewDDL(conflation_version, year);

    await dama_db.query(ddl);
  }, pg_env);
}
