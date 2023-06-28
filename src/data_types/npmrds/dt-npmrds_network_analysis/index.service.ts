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
      "npmrds_network_spatial_analysis",
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
      "npmrds_network_spatial_analysis",
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
      "npmrds_network_spatial_analysis",
      `tmc_network_edges_${year}`,

      // c
      "npmrds_network_spatial_analysis",
      `tmc_network_edges_${year}`,

      // c ON
      v,
      w,

      // e
      "npmrds_network_spatial_analysis",
      `tmc_network_edges_${year}`,

      // f
      "npmrds_network_spatial_analysis",
      `tmc_network_edges_${year}`,

      // f ON
      v,
      w,

      // g
      "npmrds_network_spatial_analysis",
      `tmc_shapes_${year}`
    )
  );

  return sql;
}

export default {
  name: serviceName,

  actions: {
    // NOTE: Queues a batch ETL worker, not aggregate.
    getTmcs: {
      visibility: "published",

      async handler(ctx: MoleculerContext) {
        // @ts-ignore
        const { params }: { params: object } = ctx;

        // @ts-ignore
        const { dependency_cells_meta } = params;

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
                    tmc
                  FROM %I
            `,
            `cte_${last_cell_id}`
          )
        );

        const { rows } = await dama_db.query({ text: sql, values });

        const tmcs = rows.map(({ tmc }) => tmc);
        return tmcs;
      },
    },

    getTmcFeatures: {
      visibility: "published",

      async handler(ctx: MoleculerContext) {
        // @ts-ignore
        const { params }: { params: object } = ctx;

        // @ts-ignore
        const { year, tmcs } = params;

        const sql = dedent(
          pgFormat(
            `
              SELECT
                tmc,
                json_build_object(
                  'type',         'Feature',
                  'geometry',     ST_AsGeoJSON(wkb_geometry)::jsonb
                ) AS feature
                FROM %I.%I AS a
                  INNER JOIN (
                    SELECT
                        tmc
                      FROM UNNEST($1::TEXT[]) AS t(tmc)
                  ) AS b USING (tmc)
            `,
            "npmrds_network_spatial_analysis",
            `tmc_shapes_${year}`
          )
        );

        const { rows } = await dama_db.query({ text: sql, values: [tmcs] });

        const features_by_id = rows.reduce((acc, { tmc, feature }) => {
          feature.properties = { tmc };

          acc[tmc] = feature;

          return acc;
        }, {});

        return features_by_id;
      },
    },
  },
};
