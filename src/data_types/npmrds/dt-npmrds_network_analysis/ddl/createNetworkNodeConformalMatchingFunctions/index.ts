import { readFile as readFileAsync } from "fs/promises";
import { join } from "path";

import pgFormat from "pg-format";
import dedent from "dedent";

import dama_db from "data_manager/dama_db";
import logger from "data_manager/logger";

import { network_spatial_analysis_schema_name } from "../utils";

async function createConformalMatchingFunction() {
  const template_sql_fpath = join(
    __dirname,
    "./sql/create_conformal_matching_fn.sql"
  );

  const template_sql = await readFileAsync(template_sql_fpath, {
    encoding: "utf8",
  });

  const sql = template_sql.replace(
    /__NETWORK_SPATIAL_ANALYSIS_SCHEMA_NAME__/g,
    network_spatial_analysis_schema_name
  );

  await dama_db.query(sql);
  logger.debug("Create level_1_conformal_matches: DONE");
}

async function createLevel1ConformalMatchingFunction() {
  logger.debug("Create level_1_conformal_matches: START");

  const sql = dedent(
    pgFormat(
      `
        -- NOTE: Can use TEMP Tables as fucntion params for API queries.

        CREATE OR REPLACE FUNCTION %I.level_1_conformal_matches (
          incident_edge_metadata_a REGCLASS,
          incident_edge_metadata_b REGCLASS,
          spatial_sensitivity REAL DEFAULT 10
        )
          RETURNS TABLE (
            node_id_a   INTEGER,
            node_id_b   INTEGER
          )

          LANGUAGE plpgsql AS

          $func$
            BEGIN

              RETURN QUERY EXECUTE FORMAT(
                '
                  SELECT DISTINCT
                      x.node_id_a,
                      x.node_id_b
                    FROM (
                      SELECT
                          a.node_id AS node_id_a,
                          a.wkb_geometry::TEXT AS wkb_geometry_a,

                          b.node_id AS node_id_b,
                          b.wkb_geometry::TEXT AS wkb_geometry_b,

                          (
                            json_build_array(
                              json_agg(
                                json_build_array(a.tmc, a.linear_id, a.direction)
                                  ORDER BY a.bearing, a.linear_id, a.direction, a.tmc
                              ) FILTER (
                                WHERE a.traversal_direction = ''INBOUND''
                              ),

                              json_agg(
                                json_build_array(a.tmc, a.linear_id, a.direction)
                                  ORDER BY a.bearing, a.linear_id, a.direction, a.tmc
                              ) FILTER (
                                WHERE a.traversal_direction = ''OUTBOUND''
                              )
                            )::TEXT
                          ) AS label

                        FROM %%s AS a
                          INNER JOIN %%s AS b
                            ON (
                              ( a.tmc = b.tmc )
                              AND
                              ( a.linear_id = b.linear_id )
                              AND
                              ( a.direction = b.direction )
                              AND
                              ( a.traversal_direction = b.traversal_direction )
                              AND
                              ( ABS( a.bearing - b.bearing ) < ( 5.0 / %%s ) /*degrees*/ )
                              AND
                              (
                                public.ST_Distance(
                                  a.wkb_geometry::public.geometry,
                                  b.wkb_geometry::public.geometry
                                ) < ( 5 / %%s ) /*meter*/
                              )
                            )
                        GROUP BY 1, 2, 3, 4
                    -- FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME
                    -- I think y is redundant.
                    ) AS x
                      INNER JOIN (
                        SELECT
                            node_id AS node_id_a,
                            wkb_geometry::TEXT AS wkb_geometry_a,

                            (
                              json_build_array(
                                json_agg(
                                  json_build_array(tmc, linear_id, direction)
                                    ORDER BY bearing, linear_id, direction, tmc
                                ) FILTER (
                                  WHERE traversal_direction = ''INBOUND''
                                ),

                                json_agg(
                                  json_build_array(tmc, linear_id, direction)
                                    ORDER BY bearing, linear_id, direction, tmc
                                ) FILTER (
                                  WHERE traversal_direction = ''OUTBOUND''
                                )
                              )::TEXT
                            ) AS label
                          FROM %%s
                          GROUP BY 1, 2
                      ) AS y
                        USING ( node_id_a, wkb_geometry_a, label )
                      INNER JOIN (
                        SELECT
                            node_id AS node_id_b,
                            wkb_geometry::TEXT AS wkb_geometry_b,

                            (
                              json_build_array(
                                json_agg(
                                  json_build_array(tmc, linear_id, direction)
                                    ORDER BY bearing, linear_id, direction, tmc
                                ) FILTER (
                                  WHERE traversal_direction = ''INBOUND''
                                ),

                                json_agg(
                                  json_build_array(tmc, linear_id, direction)
                                    ORDER BY bearing, linear_id, direction, tmc
                                ) FILTER (
                                  WHERE traversal_direction = ''OUTBOUND''
                                )
                              )::TEXT
                            ) AS label
                          FROM %%s
                          GROUP BY 1, 2
                      ) AS z
                        USING ( node_id_b, wkb_geometry_b, label )
                        
                ',
                incident_edge_metadata_a,
                incident_edge_metadata_b,
                spatial_sensitivity,
                spatial_sensitivity,
                incident_edge_metadata_a,
                incident_edge_metadata_b
            ) ;

            END ;

          $func$
        ;
      `,
      network_spatial_analysis_schema_name
    )
  );

  await dama_db.query(sql);
  logger.debug("Create level_1_conformal_matches: DONE");
}

async function createLevel2ConformalMatchingFunction() {
  logger.debug("Create level_2_conformal_matches: START");

  const sql = dedent(
    pgFormat(
      `
        -- NOTE: Can use TEMP Tables as fucntion params for API queries.

        CREATE OR REPLACE FUNCTION %I.level_2_conformal_matches (
          incident_edge_metadata_a regclass,
          incident_edge_metadata_b regclass,
          spatial_sensitivity integer default 10
        )
          RETURNS TABLE (
            node_id_a   INTEGER,
            node_id_b   INTEGER
          )

          LANGUAGE plpgsql AS

          $func$
            BEGIN

              RETURN QUERY EXECUTE FORMAT(
                '
                  SELECT DISTINCT
                      x.node_id_a,
                      x.node_id_b
                    FROM (
                      SELECT
                          a.node_id AS node_id_a,
                          b.node_id AS node_id_b,
                          (
                            json_build_array(
                              json_agg(
                                json_build_array(a.linear_id, a.direction)
                                  ORDER BY a.bearing, a.linear_id, a.direction
                              ) FILTER (
                                WHERE a.traversal_direction = ''INBOUND''
                              ),

                              json_agg(
                                json_build_array(a.linear_id, a.direction)
                                  ORDER BY a.bearing, a.linear_id, a.direction
                              ) FILTER (
                                WHERE a.traversal_direction = ''OUTBOUND''
                              )
                            )::TEXT
                          ) AS label

                        FROM %%s AS a
                          INNER JOIN %%s AS b
                            ON (
                              ( a.linear_id = b.linear_id )
                              AND
                              ( a.direction = b.direction )
                              AND
                              ( a.traversal_direction = b.traversal_direction )
                              AND
                              ( ABS( a.bearing - b.bearing ) < ( 5 / %%s ) /*degrees*/ )
                              AND
                              (
                                public.ST_Distance(
                                  a.wkb_geometry::public.geometry,
                                  b.wkb_geometry::public.geometry
                                ) < ( 5 / %%s ) /*meter*/
                              )
                            )
                        GROUP BY 1, 2
                    ) AS x
                      INNER JOIN (
                        SELECT
                            node_id AS node_id_a,
                            (
                              json_build_array(
                                json_agg(
                                  json_build_array(linear_id, direction)
                                    ORDER BY bearing, linear_id, direction
                                ) FILTER (
                                  WHERE traversal_direction = ''INBOUND''
                                ),

                                json_agg(
                                  json_build_array(linear_id, direction)
                                    ORDER BY bearing, linear_id, direction
                                ) FILTER (
                                  WHERE traversal_direction = ''OUTBOUND''
                                )
                              )::TEXT
                            ) AS label
                          FROM %%s
                          GROUP BY 1
                      ) AS y
                        USING ( node_id_a, label )
                      INNER JOIN (
                        SELECT
                            node_id AS node_id_b,
                            (
                              json_build_array(
                                json_agg(
                                  json_build_array(linear_id, direction)
                                    ORDER BY bearing, linear_id, direction
                                ) FILTER (
                                  WHERE traversal_direction = ''INBOUND''
                                ),

                                json_agg(
                                  json_build_array(linear_id, direction)
                                    ORDER BY bearing, linear_id, direction
                                ) FILTER (
                                  WHERE traversal_direction = ''OUTBOUND''
                                )
                              )::TEXT
                            ) AS label
                          FROM %%s
                          GROUP BY 1
                      ) AS z
                        USING ( node_id_b, label )
                        
                ',
                incident_edge_metadata_a,
                incident_edge_metadata_b,
                spatial_sensitivity,
                spatial_sensitivity,
                incident_edge_metadata_a,
                incident_edge_metadata_b
            ) ;

            END ;

          $func$
        ;
      `,
      network_spatial_analysis_schema_name
    )
  );

  await dama_db.query(sql);
  logger.debug("Create level_2_conformal_matches: DONE");
}

async function createLevel3ConformalMatchingFunction() {
  logger.debug("Create level_3_conformal_matches: START");

  const sql = dedent(
    pgFormat(
      `
        -- NOTE: Can use TEMP Tables as fucntion params for API queries.

        CREATE OR REPLACE FUNCTION %I.level_3_conformal_matches (
          incident_edge_metadata_a regclass,
          incident_edge_metadata_b regclass,
          spatial_sensitivity integer default 10
        )
          RETURNS TABLE (
            node_id_a   INTEGER,
            node_id_b   INTEGER
          )

          LANGUAGE plpgsql AS

          $func$
            BEGIN

              RETURN QUERY EXECUTE FORMAT(
                '
                  SELECT DISTINCT
                      x.node_id_a,
                      x.node_id_b
                    FROM (
                      SELECT
                          a.node_id AS node_id_a,
                          b.node_id AS node_id_b,
                          (
                            json_build_array(
                              json_agg(
                                json_build_array(a.linear_id)
                                  ORDER BY a.bearing, a.linear_id, a.direction
                              ) FILTER (
                                WHERE a.traversal_direction = ''INBOUND''
                              ),

                              json_agg(
                                json_build_array(a.linear_id)
                                  ORDER BY a.bearing, a.linear_id, a.direction
                              ) FILTER (
                                WHERE a.traversal_direction = ''OUTBOUND''
                              )
                            )::TEXT
                          ) AS label

                        FROM %%s AS a
                          INNER JOIN %%s AS b
                            ON (
                              ( a.linear_id = b.linear_id )
                              AND
                              ( a.traversal_direction = b.traversal_direction )
                              AND
                              ( ABS( a.bearing - b.bearing ) < ( 20 / %%s ) /*degrees*/ )
                              AND
                              (
                                public.ST_Distance(
                                  a.wkb_geometry::public.geometry,
                                  b.wkb_geometry::public.geometry
                                ) < ( 20 / %%s ) /*meter*/
                              )
                            )
                        GROUP BY 1, 2
                    ) AS x
                      INNER JOIN (
                        SELECT
                            node_id AS node_id_a,
                            (
                              json_build_array(
                                json_agg(
                                  json_build_array(linear_id)
                                    ORDER BY bearing, linear_id, direction
                                ) FILTER (
                                  WHERE traversal_direction = ''INBOUND''
                                ),

                                json_agg(
                                  json_build_array(linear_id)
                                    ORDER BY bearing, linear_id, direction
                                ) FILTER (
                                  WHERE traversal_direction = ''OUTBOUND''
                                )
                              )::TEXT
                            ) AS label
                          FROM %%s
                          GROUP BY 1
                      ) AS y
                        USING ( node_id_a, label )
                      INNER JOIN (
                        SELECT
                            node_id AS node_id_b,
                            (
                              json_build_array(
                                json_agg(
                                  json_build_array(linear_id)
                                    ORDER BY bearing, linear_id, direction
                                ) FILTER (
                                  WHERE traversal_direction = ''INBOUND''
                                ),

                                json_agg(
                                  json_build_array(linear_id)
                                    ORDER BY bearing, linear_id, direction
                                ) FILTER (
                                  WHERE traversal_direction = ''OUTBOUND''
                                )
                              )::TEXT
                            ) AS label
                          FROM %%s
                          GROUP BY 1
                      ) AS z
                        USING ( node_id_b, label )
                        
                ',
                incident_edge_metadata_a,
                incident_edge_metadata_b,
                spatial_sensitivity,
                spatial_sensitivity,
                incident_edge_metadata_a,
                incident_edge_metadata_b
            ) ;

            END ;

          $func$
        ;
      `,
      network_spatial_analysis_schema_name
    )
  );

  await dama_db.query(sql);
  logger.debug("Create level_3_conformal_matches: DONE");
}

async function createLevel4ConformalMatchingFunction() {
  logger.debug("Create level_3_conformal_matches: START");

  const sql = dedent(
    pgFormat(
      `
        -- NOTE: Can use TEMP Tables as fucntion params for API queries.

        CREATE OR REPLACE FUNCTION %I.level_3_conformal_matches (
          incident_edge_metadata_a regclass,
          incident_edge_metadata_b regclass,
          spatial_sensitivity integer default 10
        )
          RETURNS TABLE (
            node_id_a   INTEGER,
            node_id_b   INTEGER
          )

          LANGUAGE plpgsql AS

          $func$
            BEGIN

              RETURN QUERY EXECUTE FORMAT(
                '
                  SELECT DISTINCT
                      x.node_id_a,
                      x.node_id_b
                    FROM (
                      SELECT
                          a.node_id AS node_id_a,
                          b.node_id AS node_id_b,
                          (
                            json_build_array(
                              json_agg(
                                json_build_array(a.linear_id)
                                  ORDER BY a.bearing, a.linear_id, a.direction
                              ) FILTER (
                                WHERE a.traversal_direction = ''INBOUND''
                              ),

                              json_agg(
                                json_build_array(a.linear_id)
                                  ORDER BY a.bearing, a.linear_id, a.direction
                              ) FILTER (
                                WHERE a.traversal_direction = ''OUTBOUND''
                              )
                            )::TEXT
                          ) AS label

                        FROM %%s AS a
                          INNER JOIN %%s AS b
                            ON (
                              ( a.linear_id = b.linear_id )
                              AND
                              ( a.traversal_direction = b.traversal_direction )
                              AND
                              (
                                public.ST_Distance(
                                  a.wkb_geometry::public.geometry,
                                  b.wkb_geometry::public.geometry
                                ) < ( 20 / %%s ) /*meter*/
                              )
                            )
                        GROUP BY 1, 2
                    ) AS x
                      INNER JOIN (
                        SELECT
                            node_id AS node_id_a,
                            (
                              json_build_array(
                                json_agg(
                                  json_build_array(linear_id)
                                    ORDER BY bearing, linear_id, direction
                                ) FILTER (
                                  WHERE traversal_direction = ''INBOUND''
                                ),

                                json_agg(
                                  json_build_array(linear_id)
                                    ORDER BY bearing, linear_id, direction
                                ) FILTER (
                                  WHERE traversal_direction = ''OUTBOUND''
                                )
                              )::TEXT
                            ) AS label
                          FROM %%s
                          GROUP BY 1
                      ) AS y
                        USING ( node_id_a, label )
                      INNER JOIN (
                        SELECT
                            node_id AS node_id_b,
                            (
                              json_build_array(
                                json_agg(
                                  json_build_array(linear_id)
                                    ORDER BY bearing, linear_id, direction
                                ) FILTER (
                                  WHERE traversal_direction = ''INBOUND''
                                ),

                                json_agg(
                                  json_build_array(linear_id)
                                    ORDER BY bearing, linear_id, direction
                                ) FILTER (
                                  WHERE traversal_direction = ''OUTBOUND''
                                )
                              )::TEXT
                            ) AS label
                          FROM %%s
                          GROUP BY 1
                      ) AS z
                        USING ( node_id_b, label )
                        
                ',
                incident_edge_metadata_a,
                incident_edge_metadata_b,
                spatial_sensitivity,
                incident_edge_metadata_a,
                incident_edge_metadata_b
            ) ;

            END ;

          $func$
        ;
      `,
      network_spatial_analysis_schema_name
    )
  );

  await dama_db.query(sql);
  logger.debug("Create level_3_conformal_matches: DONE");
}

export default async function createNetworkNodeLevel1Labels() {
  await createLevel1ConformalMatchingFunction();
  await createLevel2ConformalMatchingFunction();
  await createLevel3ConformalMatchingFunction();
}
