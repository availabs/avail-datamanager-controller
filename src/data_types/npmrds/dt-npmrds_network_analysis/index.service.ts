import dedent from "dedent";
import pgFormat from "pg-format";
import _ from "lodash";

import { Context as MoleculerContext } from "moleculer";

import dama_db from "data_manager/dama_db";

export const serviceName = "dama/data_types/npmrds/network-analysis";

type MapYearDescriptor = {
  year: number;
  map: string;
};

type MapFilterDescriptor = {
  property_name: string;
  property_value: string;
};

function getMapYearSubQuery({ cell_id, descriptor: { year } }) {
  const sql = dedent(
    pgFormat(
      `
        %I AS (
          SELECT
              tmc
            FROM %I.%I
        )
      `,
      `cte_${cell_id}`,
      "npmrds_tmc_shapes_analysis",
      `tmc_shapes_${year}`
    )
  );

  return sql;
}

function getMapFilterSubQuery(
  { cell_id, dependencies, descriptor: { property_name, property_value } },
  year: number,
  values: any[]
) {
  const v =
    property_value !== null && Number.isFinite(+property_value)
      ? +property_value
      : property_value;

  values.push(v);

  const sql = dedent(
    pgFormat(
      `
        %I AS ( 
          SELECT
              tmc
            FROM %I
              INNER JOIN %I.%I
                USING (tmc)
            WHERE ( %I = $${values.length} )
        )
      `,
      `cte_${cell_id}`,
      `cte_${dependencies[0]}`,
      "npmrds_tmc_shapes_analysis",
      `tmc_shapes_${year}`,
      property_name
    )
  );

  return sql;
}

function getMapTraverseSubQuery(
  { cell_id, dependencies, descriptor: { direction, distance } },
  year: number,
  values: any[]
) {
  values.push(distance);

  const v = direction === "INBOUND" ? "end_node_id" : "start_node_id";
  const w = direction === "INBOUND" ? "start_node_id" : "end_node_id";

  // TODO: Handle direction
  const sql = dedent(
    pgFormat(
      `
        %I AS ( 
            WITH RECURSIVE cte_traverse AS (
              SELECT
                c.tmc,
                0::DOUBLE PRECISION AS miles
              FROM %I AS a
                INNER JOIN %I.%I AS b
                  USING (tmc)
                INNER JOIN %I.%I AS c
                  ON ( b.%I = c.%I )
            UNION ALL
            SELECT DISTINCT ON (tmc)
                f.tmc,
                (d.miles + g.miles) AS miles
              FROM cte_traverse AS d
                INNER JOIN %I.%I AS e
                  USING (tmc)
                INNER JOIN %I.%I AS f
                  ON ( e.%I = f.%I )
                INNER JOIN %I.%I AS g
                  ON ( f.tmc = g.tmc )
              WHERE ( d.miles < $${values.length} )
          )
            SELECT DISTINCT
                tmc
              FROM cte_traverse
        )
      `,
      `cte_${cell_id}`,

      // a
      `cte_${dependencies[0]}`,

      // b
      "npmrds_tmc_shapes_analysis",
      `tmc_network_edges_${year}`,

      // c
      "npmrds_tmc_shapes_analysis",
      `tmc_network_edges_${year}`,

      // c ON
      v,
      w,

      // e
      "npmrds_tmc_shapes_analysis",
      `tmc_network_edges_${year}`,

      // f
      "npmrds_tmc_shapes_analysis",
      `tmc_network_edges_${year}`,

      // f ON
      v,
      w,

      // g
      "npmrds_tmc_shapes_analysis",
      `tmc_shapes_${year}`
    )
  );

  return sql;
}

export default {
  name: serviceName,

  actions: {
    // NOTE: Queues a batch ETL worker, not aggregate.
    getTmcFeatures: {
      visibility: "published",

      async handler(ctx: MoleculerContext) {
        // @ts-ignore
        const { params }: { params: object } = ctx;

        // @ts-ignore
        const { dependency_cells_meta } = params;

        console.log("$".repeat(100));
        console.log(JSON.stringify({ dependency_cells_meta }, null, 4));
        console.log("$".repeat(100));

        const [
          {
            descriptor: { year },
          },
        ] = dependency_cells_meta;

        const ctes: string[] = [];
        const values = [];

        for (const meta of dependency_cells_meta) {
          if (meta.cell_type === "Map Year Cell") {
            ctes.push(getMapYearSubQuery(meta));
          } else if (meta.cell_type === "Map Filter Cell") {
            ctes.push(getMapFilterSubQuery(meta, year, values));
          } else if (meta.cell_type === "Map Traverse Cell") {
            ctes.push(getMapTraverseSubQuery(meta, year, values));
          } else {
            throw Error("Unrecognized cell_type");
          }
        }

        // @ts-ignore
        const { cell_id: last_cell_id } = _.last(dependency_cells_meta);

        const sql = dedent(
          pgFormat(
            `
              WITH ${ctes}
                SELECT
                  jsonb_build_object(
                    'type',       'FeatureCollection',
                    'features',   jsonb_agg(
                                    json_build_object(
                                      'type',         'Feature',
                                      'properties',   to_jsonb(b.*) - 'wkb_geometry',
                                      'geometry',     ST_AsGeoJSON(wkb_geometry)::jsonb
                                    )
                                  )
                  ) as tmc_feature_collection
                  FROM %I AS a
                    INNER JOIN %I.%I AS b
                      USING (tmc)
            `,
            `cte_${last_cell_id}`,
            "npmrds_tmc_shapes_analysis",
            `tmc_shapes_${year}`
          )
        );

        console.log(sql);

        const {
          rows: [{ tmc_feature_collection }],
        } = await dama_db.query({ text: sql, values });

        return tmc_feature_collection;
      },
    },
  },
};
