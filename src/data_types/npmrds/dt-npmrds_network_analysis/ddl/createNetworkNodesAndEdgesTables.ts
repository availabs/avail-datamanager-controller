import pgFormat from "pg-format";
import dedent from "dedent";

import dama_db from "data_manager/dama_db";
import logger from "data_manager/logger";

import {
  temporary_tmc_points_table_name,
  coord_rounding_precision,
  getTmcShapesTableInfo,
  getNpmrdsNetworkNodesTableInfo,
  getNpmrdsNetworkEdgesTableInfo,
  getNpmrdsNetworkEdgeMaxGeomIdxTableInfo,
  getNpmrdsNetworkEdgesMetadataViewInfo,
} from "./utils";

async function createTemporaryTmcPointsTable(year: number) {
  const tmc_shapes_table_info = getTmcShapesTableInfo(year);

  const create_sql = dedent(
    pgFormat(
      `
        CREATE TEMPORARY TABLE tmp_tmc_points (
          tmc           TEXT,
          pt_geom_n     INTEGER NOT NULL,
          pt_geom_idx   INTEGER NOT NULL,
          longitude     NUMERIC NOT NULL,
          latitude      NUMERIC NOT NULL
        ) 
          WITH (fillfactor=100) 
        ;
      `
    )
  );

  await dama_db.query(create_sql);

  logger.debug(`Created tmp_tmc_points for ${year}`);

  const load_sql = dedent(
    pgFormat(
      `
        INSERT INTO tmp_tmc_points (
          tmc,
          pt_geom_n,
          pt_geom_idx,
          longitude,
          latitude
        )
          SELECT 
              tmc,
              (pt_dump).path[1] AS pt_geom_n,
              (pt_dump).path[2] AS pt_geom_idx,
              ROUND(
                ST_X((pt_dump).geom)::NUMERIC,
                $1
              ) AS longitude,
              ROUND(
                ST_Y((pt_dump).geom)::NUMERIC,
                $1
              ) AS latitude
            FROM (
              SELECT
                  tmc,
                  ST_DumpPoints(wkb_geometry) AS pt_dump
                FROM %I.%I
            ) AS t
        ;
      `,
      tmc_shapes_table_info.table_schema,
      tmc_shapes_table_info.table_name
    )
  );

  await dama_db.query({ text: load_sql, values: [coord_rounding_precision] });

  logger.debug(`Loaded tmp_tmc_points for ${year}`);

  const finish_sql = dedent(
    pgFormat(
      `
        CREATE INDEX tmp_tmc_points_coords_idx
          ON tmp_tmc_points (longitude, latitude)
        ;

        CLUSTER tmp_tmc_points USING tmp_tmc_points_coords_idx ;
      `
    )
  );

  await dama_db.query(finish_sql);
}

async function createNpmrdsNetworkNodesTable(year: number) {
  const nodes_table_info = getNpmrdsNetworkNodesTableInfo(year);

  logger.debug(`Create ${nodes_table_info.table_name}: START.`);

  const sql = dedent(
    pgFormat(
      `
        DROP TABLE IF EXISTS %I.%I CASCADE ;

        CREATE TABLE IF NOT EXISTS %I.%I (
          node_id         SERIAL PRIMARY KEY,

          longitude       NUMERIC NOT NULL,
          latitude        NUMERIC NOT NULL,

          wkb_geometry    public.geometry(Point, 4326),

          UNIQUE (longitude, latitude)
        ) ;

        INSERT INTO %I.%I (
          longitude,
          latitude,
          wkb_geometry
        )
          SELECT DISTINCT
              longitude,
              latitude,
              ST_SetSRID(
                ST_MakePoint(
                  longitude,
                  latitude
                ),
                4326
              ) AS wkb_geometry
            FROM %I
          ORDER BY longitude, latitude
        ;

        CREATE INDEX %I
          ON %I.%I (longitude, latitude)
        ;

        CLUSTER %I.%I
          USING %I
        ;
      `,

      // DROP TABLE
      nodes_table_info.table_schema,
      nodes_table_info.table_name,

      // CREATE TABLE
      nodes_table_info.table_schema,
      nodes_table_info.table_name,

      // INSERT INTO
      nodes_table_info.table_schema,
      nodes_table_info.table_name,
      temporary_tmc_points_table_name,

      // CREATE COORD INDEX
      nodes_table_info.coord_idx_name,
      nodes_table_info.table_schema,
      nodes_table_info.table_name,

      // CLUSTER
      nodes_table_info.table_schema,
      nodes_table_info.table_name,
      nodes_table_info.pkey_idx_name
    )
  );

  await dama_db.query(sql);

  logger.debug(`Create ${nodes_table_info.table_name}: DONE.`);
}

async function createNpmrdsNetworkEdgesTable(year: number) {
  const nodes_table_info = getNpmrdsNetworkNodesTableInfo(year);
  const edges_table_info = getNpmrdsNetworkEdgesTableInfo(year);

  logger.debug(`Create ${edges_table_info.table_name}: START`);

  const sql = dedent(
    pgFormat(
      `
        DROP TABLE IF EXISTS %I.%I CASCADE;

        CREATE TABLE %I.%I (
          tmc           TEXT,
          pt_geom_n     INTEGER,
          pt_geom_idx   INTEGER,
          node_id       INTEGER NOT NULL,

          PRIMARY KEY (tmc, pt_geom_n, pt_geom_idx)
        ) WITH (fillfactor=100) ;

        INSERT INTO %I.%I (
          tmc,
          pt_geom_n,
          pt_geom_idx,
          node_id
        )
          SELECT
              a.tmc,
              a.pt_geom_n,
              a.pt_geom_idx,
              b.node_id
            FROM tmp_tmc_points AS a
              INNER JOIN %I.%I AS b
                USING (longitude, latitude)
        ;

        CREATE INDEX %I
          ON %I.%I (node_id)
        ;

        CLUSTER %I.%I
          USING %I
        ;
      `,

      // DROP TABLE
      edges_table_info.table_schema,
      edges_table_info.table_name,

      // CREATE TABLE
      edges_table_info.table_schema,
      edges_table_info.table_name,

      // INSERT INTO
      edges_table_info.table_schema,
      edges_table_info.table_name,
      nodes_table_info.table_schema,
      nodes_table_info.table_name,

      // CREATE INDEX
      edges_table_info.node_idx_name,
      edges_table_info.table_schema,
      edges_table_info.table_name,

      // CLUSTER
      edges_table_info.table_schema,
      edges_table_info.table_name,
      edges_table_info.pkey_idx_name
    )
  );

  await dama_db.query(sql);

  logger.debug(`Create ${edges_table_info.table_name}: DONE`);
}

async function createNpmrdsNetworkEdgesMaxPointGeomIdxTable(year: number) {
  const edges_table_info = getNpmrdsNetworkEdgesTableInfo(year);
  const max_geom_idx_table_info = getNpmrdsNetworkEdgeMaxGeomIdxTableInfo(year);

  logger.debug(`Create ${max_geom_idx_table_info.table_name}: START`);

  const sql = dedent(
    pgFormat(
      `
        DROP TABLE IF EXISTS %I.%I CASCADE ;

        CREATE TABLE %I.%I (
          tmc               TEXT,
          pt_geom_n         INTEGER,
          max_pt_geom_idx   INTEGER,

          PRIMARY KEY (tmc, pt_geom_n)
        ) WITH (fillfactor=100) ;

        INSERT INTO %I.%I (
          tmc,
          pt_geom_n,
          max_pt_geom_idx
        )
          SELECT
              tmc,
              pt_geom_n,
              MAX(pt_geom_idx) AS max_pt_geom_idx
            FROM %I.%I
            GROUP BY 1,2
        ;

        CLUSTER %I.%I
          USING %I
        ;
      `,

      // DROP TABLE
      max_geom_idx_table_info.table_schema,
      max_geom_idx_table_info.table_name,

      // CREATE TABLE
      max_geom_idx_table_info.table_schema,
      max_geom_idx_table_info.table_name,

      // INSERT INTO
      max_geom_idx_table_info.table_schema,
      max_geom_idx_table_info.table_name,
      edges_table_info.table_schema,
      edges_table_info.table_name,

      // CLUSTER
      max_geom_idx_table_info.table_schema,
      max_geom_idx_table_info.table_name,
      max_geom_idx_table_info.pkey_idx_name
    )
  );

  await dama_db.query(sql);

  logger.debug(`Create ${max_geom_idx_table_info.table_name}: DONE`);
}

async function createNpmrdsNetworkEdgesMetadataView(year: number) {
  const tmc_shapes_info = getTmcShapesTableInfo(year);
  const edges_table_info = getNpmrdsNetworkEdgesTableInfo(year);
  const max_geom_idx_table_info = getNpmrdsNetworkEdgeMaxGeomIdxTableInfo(year);
  const edges_meta_info = getNpmrdsNetworkEdgesMetadataViewInfo(year);

  logger.debug(`Create ${edges_meta_info.table_name}: START`);

  const sql = dedent(
    pgFormat(
      `
        DROP VIEW IF EXISTS %I.%I ;

        CREATE OR REPLACE VIEW %I.%I
          AS
            SELECT
                a.*,
                b.node_id AS start_node_id,
                c.node_id AS end_node_id,
                public.ST_Length(
                  public.GEOMETRY(
                    wkb_geometry
                  )
                ) AS length_meters
              FROM %I.%I AS a             -- tmc_shapes
                INNER JOIN %I.%I AS b     -- npmrds_network_edges
                  USING (tmc)
                INNER JOIN %I.%I AS c     -- npmrds_network_edges
                  USING (tmc)
                INNER JOIN %I.%I AS d     -- npmrds_network_edges_max_geom_idx
                  USING (tmc)
              WHERE (
                ( b.pt_geom_n = 1 )
                AND
                ( b.pt_geom_idx = 1 )
                AND
                ( c.pt_geom_n = 1 )
                AND
                ( d.pt_geom_n = 1 )
                AND
                ( c.pt_geom_idx = d.max_pt_geom_idx )
              )
        ;
      `,

      // DROP VIEW
      edges_meta_info.table_schema,
      edges_meta_info.table_name,

      // CREATE VIEW
      edges_meta_info.table_schema,
      edges_meta_info.table_name,

      // FROM AS a
      tmc_shapes_info.table_schema,
      tmc_shapes_info.table_name,

      // INNER JOIN AS b
      edges_table_info.table_schema,
      edges_table_info.table_name,

      // INNER JOIN AS c
      edges_table_info.table_schema,
      edges_table_info.table_name,

      // INNER JOIN AS d
      max_geom_idx_table_info.table_schema,
      max_geom_idx_table_info.table_name
    )
  );

  await dama_db.query(sql);

  logger.debug(`Create ${edges_meta_info.table_name}: DONE`);
}

export default async function createNetworkNodesAndEdgesTables(year: number) {
  await createTemporaryTmcPointsTable(year);
  await createNpmrdsNetworkNodesTable(year);
  await createNpmrdsNetworkEdgesTable(year);
  await createNpmrdsNetworkEdgesMaxPointGeomIdxTable(year);
  await createNpmrdsNetworkEdgesMetadataView(year);
}
