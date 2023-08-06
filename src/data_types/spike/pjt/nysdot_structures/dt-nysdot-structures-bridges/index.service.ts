import dedent from "dedent";
import _ from "lodash";

import { Context as MoleculerContext } from "moleculer";

import dama_db from "data_manager/dama_db";

export const serviceName =
  "dama/data_types/nysdot_structures/dt-nysdot-structures-bridges";

export default {
  name: serviceName,

  actions: {
    getBridgesMetadataForCounty: {
      visibility: "published",

      async handler(ctx: MoleculerContext) {
        // @ts-ignore
        const {
          // @ts-ignore
          params: { county },
        } = ctx;

        const sql = dedent(
          `
            SELECT
                ogc_fid,
                object_id,
                bin,
                location_la,
                carried,
                crossed,
                primary_own,
                primary_mai,
                county,
                region,
                gtms_struct,
                gtms_materi,
                number_of_sp,
                condition_r,
                last_inspec,
                bridge_leng,
                deck_area_sq,
                aadt,
                year_built,
                posted_load,
                r_posted,
                other,
                redc,
                nbi_deck_co,
                nbi_substr,
                nbi_supers,
                fhwa_condi
              FROM nysdot_structures.bridges
              WHERE ( county = UPPER($1) )
          `
        );

        const { rows } = await dama_db.query({ text: sql, values: [county] });

        const metadata_by_id = rows.reduce((acc, row) => {
          acc[row.ogc_fid] = row;

          return acc;
        }, {});

        return metadata_by_id;
      },
    },
  },
};
