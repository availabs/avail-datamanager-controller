import dedent from "dedent";
import pgFormat from "pg-format";

import dama_db from "data_manager/dama_db";
import logger from "data_manager/logger";

import {
  getNpmrdsTmcShapeSimilarityTableInfo,
  getTmcShapesTableInfo,
} from "./utils";

const min_year = 2017;
const max_year = 2022;

async function createTmcShapeSimilarityTable(year_a: number, year_b: number) {
  const tmc_shapes_info_a = getTmcShapesTableInfo(year_a);
  const tmc_shapes_info_b = getTmcShapesTableInfo(year_b);

  const tmc_similarity_info = getNpmrdsTmcShapeSimilarityTableInfo(
    year_a,
    year_b
  );

  const create_table_sql = dedent(
    pgFormat(
      `
        DROP TABLE IF EXISTS %I.%I CASCADE ;

        CREATE TABLE %I.%I (
          tmc                             TEXT PRIMARY KEY,

          length_meters_a                 DOUBLE PRECISION NOT NULL,
          length_meters_b                 DOUBLE PRECISION NOT NULL,

          straight_line_dist_meters_a     DOUBLE PRECISION NOT NULL,
          straight_line_dist_meters_b     DOUBLE PRECISION NOT NULL,

          bearing_degrees_a               DOUBLE PRECISION,
          bearing_degrees_b               DOUBLE PRECISION,

          -- https://en.wikipedia.org/wiki/Sinuosity
          sinuosity_a                     DOUBLE PRECISION,
          sinuosity_b                     DOUBLE PRECISION,

          start_pt_diff_meters            DOUBLE PRECISION NOT NULL,
          end_pt_diff_meters              DOUBLE PRECISION NOT NULL,

          -- https://en.wikipedia.org/wiki/Hausdorff_distance
          hausdorff_distance_meters       DOUBLE PRECISION NOT NULL,

          -- https://en.wikipedia.org/wiki/Fr%C3%A9chet_distance
          frechet_distance_meters         DOUBLE PRECISION NOT NULL

        ) WITH (fillfactor=100)
      `,
      tmc_similarity_info.table_schema,
      tmc_similarity_info.table_name,
      tmc_similarity_info.table_schema,
      tmc_similarity_info.table_name
    )
  );

  await dama_db.query(create_table_sql);

  const load_sql = dedent(
    pgFormat(
      `
          INSERT INTO %I.%I (
            tmc,

            length_meters_a,
            length_meters_b,

            straight_line_dist_meters_a,
            straight_line_dist_meters_b,

            bearing_degrees_a,
            bearing_degrees_b,

            sinuosity_a,
            sinuosity_b,

            start_pt_diff_meters,
            end_pt_diff_meters,

            hausdorff_distance_meters,
            frechet_distance_meters
          )
            SELECT
                tmc,

                length_meters_a,
                length_meters_b,

                straight_line_dist_meters_a,
                straight_line_dist_meters_b,

                bearing_degrees_a,
                bearing_degrees_b,

                (
                  length_meters_a 
                  /
                  NULLIF(
                    straight_line_dist_meters_a,
                    0
                  )
                ) AS sinuosity_a,

                (
                  length_meters_b 
                  /
                  NULLIF(
                    straight_line_dist_meters_b,
                    0
                  )
                ) AS sinuosity_b,

                start_pt_diff_meters,
                end_pt_diff_meters,

                hausdorff_distance_meters,
                frechet_distance_meters
              FROM (
                SELECT
                    tmc,

                    public.ST_Length(a.utm_linestring) AS length_meters_a,
                    public.ST_Length(b.utm_linestring) AS length_meters_b,

                    public.ST_Distance(
                      public.ST_StartPoint(
                        a.utm_linestring
                      ),

                      public.ST_EndPoint(
                        a.utm_linestring
                      )
                    ) AS straight_line_dist_meters_a,

                    public.ST_Distance(
                      public.ST_StartPoint(
                        b.utm_linestring
                      ),

                      public.ST_EndPoint(
                        b.utm_linestring
                      )
                    ) AS straight_line_dist_meters_b,

                    DEGREES(
                      public.ST_Azimuth(
                        public.ST_StartPoint(
                          a.utm_linestring
                        ),

                        public.ST_EndPoint(
                          a.utm_linestring
                        )
                      )
                    ) AS bearing_degrees_a,

                    DEGREES(
                      public.ST_Azimuth(
                        public.ST_StartPoint(
                          b.utm_linestring
                        ),

                        public.ST_EndPoint(
                          b.utm_linestring
                        )
                      )
                    ) AS bearing_degrees_b,

                    public.ST_Distance(
                      public.ST_StartPoint(
                        a.utm_linestring
                      ),

                      public.ST_StartPoint(
                        b.utm_linestring
                      )
                    ) AS start_pt_diff_meters,

                    public.ST_Distance(
                      public.ST_EndPoint(
                        a.utm_linestring
                      ),

                      public.ST_EndPoint(
                        b.utm_linestring
                      )
                    ) AS end_pt_diff_meters,


                    public.ST_HausdorffDistance(
                      a.utm_linestring,
                      b.utm_linestring,
                      0.25
                    ) AS hausdorff_distance_meters,

                    public.ST_FrechetDistance(
                      a.utm_linestring,
                      b.utm_linestring,
                      0.25
                    ) AS frechet_distance_meters

                  FROM (
                      SELECT
                          tmc,
                          -- NOTE: Transforming to UTM so distance units are meters and not degrees.
                          public.ST_Transform(
                            ST_GeometryN( wkb_geometry, 1 ),
                            32618                 -- https://epsg.io/32618
                          ) AS utm_linestring
                        FROM %I.%I
                    ) AS a INNER JOIN (
                      SELECT
                          tmc,
                          -- NOTE: Transforming to UTM so distance units are meters and not degrees.
                          public.ST_Transform(
                            ST_GeometryN( wkb_geometry, 1 ),
                            32618                 -- https://epsg.io/32618
                          ) AS utm_linestring
                        FROM %I.%I
                    ) AS b USING (tmc)
                ) AS t

      `,
      // INSERT INTO
      tmc_similarity_info.table_schema,
      tmc_similarity_info.table_name,
      // FROM AS a
      tmc_shapes_info_a.table_schema,
      tmc_shapes_info_a.table_name,
      // FROM AS b
      tmc_shapes_info_b.table_schema,
      tmc_shapes_info_b.table_name
    )
  );

  logger.debug(
    `Load ${tmc_similarity_info.table_name} START: ${new Date().toISOString()}`
  );

  await dama_db.query(load_sql);

  logger.debug(
    `Load ${tmc_similarity_info.table_name} DONE: ${new Date().toISOString()}`
  );
}

async function createTmcShapeSimilarityTables() {
  for (let year_a = min_year; year_a < max_year; ++year_a) {
    for (let year_b = year_a + 1; year_b <= max_year; ++year_b) {
      await createTmcShapeSimilarityTable(year_a, year_b);
    }
  }
}

export default async function createNetworkNodeLevel1Labels() {
  await createTmcShapeSimilarityTables();
}
