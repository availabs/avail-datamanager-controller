import dedent from "dedent";
import _ from "lodash";

import { Context as MoleculerContext } from "moleculer";

import dama_db from "data_manager/dama_db";

export const serviceName =
  "dama/data-types/us_geological_survey/dt-national-hydrography-dataset-flowline";

export default {
  name: serviceName,

  actions: {
    getFlowlinesByOgcFid: {
      visibility: "published",

      async handler(ctx: MoleculerContext) {
        // @ts-ignore
        let {
          // @ts-ignore
          params: { ogc_fid },
        } = ctx;

        ogc_fid = Array.isArray(ogc_fid) ? ogc_fid : [ogc_fid];

        const sql = dedent(
          `
            SELECT
                ogc_fid,
                permanent_identifier,
                fdate,
                resolution,
                gnis_id,
                gnis_name,
                lengthkm,
                reachcode,
                flowdir,
                wbarea_permanent_identifier,
                ftype,
                fcode,
                mainpath,
                innetwork,
                visibilityfilter,
                shape_length
              FROM usgs_national_hydrography_dataset.flowline AS a
                INNER JOIN (
                  SELECT
                      ogc_fid
                    FROM UNNEST($1::INTEGER[]) AS t(ogc_fid)
                ) AS b USING (ogc_fid)
            ;
          `
        );

        const { rows } = await dama_db.query({ text: sql, values: [ogc_fid] });

        return rows;
      },
    },

    getFlowlinesByCountyFipsCode: {
      visibility: "published",

      async handler(ctx: MoleculerContext) {
        // @ts-ignore
        const {
          // @ts-ignore
          params: { fips_code },
        } = ctx;

        const sql = dedent(
          `
            SELECT
                b.ogc_fid,
                b.permanent_identifier,
                b.fdate,
                b.resolution,
                b.gnis_id,
                b.gnis_name,
                b.lengthkm,
                b.reachcode,
                b.flowdir,
                b.wbarea_permanent_identifier,
                b.ftype,
                b.fcode,
                b.mainpath,
                b.innetwork,
                b.visibilityfilter,
                b.shape_length
              FROM us_census_tiger.county AS a
                INNER JOIN usgs_national_hydrography_dataset.flowline AS b
                  ON ( ST_Intersects(a.wkb_geometry, b.wkb_geometry) )
              WHERE ( a.geoid = $1 )
            ;
          `
        );

        const { rows } = await dama_db.query({
          text: sql,
          values: [fips_code],
        });

        return rows;
      },
    },
  },
};
