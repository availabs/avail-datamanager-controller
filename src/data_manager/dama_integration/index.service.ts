/*
   USEFUL:
      * https://moleculer.services/docs/0.14/actions.html#Action-visibility
          * published: public action. It can be called locally, remotely and
                    can be published via API Gateway

          * public: public action, can be called locally & remotely but not published via API GW
      * https://github.com/moleculerjs/site/blob/master/source/docs/0.14/moleculer-web.md#file-upload-aliases
*/

import { existsSync } from "fs";
import { readdir as readdirAsync } from "fs/promises";
import { join } from "path";
import { Readable } from "stream";
// import { inspect } from "util";

import { Context } from "moleculer";

import { FSA } from "flux-standard-action";

export type ServiceContext = Context & {
  params: FSA;
};

import etlDir from "../../constants/etlDir";

import GeospatialDatasetIntegrator from "../../../tasks/gis-data-integration/src/data_integrators/GeospatialDatasetIntegrator";

import PG_ENV from "../../constants/pgEnv";

// const commonDammaMetaProps = ["DAMAA", "etl_context_id", "pg_env"];

const serviceName = "dama/data_source_integrator";

export default {
  name: serviceName,

  events: {
    /*
    [`${serviceName}:LOAD_REQUEST`]: {
      context: true,
      // params: FSAEventParam,
      async handler(ctx: Context) {
        await this.stage(ctx.params);
      },
    },
    */
  },

  actions: {
    getExistingDatasetUploads: {
      visibility: "published",

      async handler() {
        const dirs = await readdirAsync(etlDir, {
          encoding: "utf8",
        });

        const ids = dirs.reduce((acc: string[], dirName: string) => {
          const workDirPath = join(etlDir, dirName);
          const path = join(workDirPath, "layerNameToId.json");

          if (existsSync(path)) {
            const id = GeospatialDatasetIntegrator.createId(workDirPath);
            acc.push(id);
          }

          return acc;
        }, []);

        return ids;
      },
    },

    uploadGeospatialDataset: {
      visibility: "published",

      async handler(ctx: Context) {
        console.log("uploadGeospatialDataset");

        console.log(JSON.stringify(ctx.meta, null, 4));
        // ctx.params.pipe(process.stdout);

        const {
          // @ts-ignore
          meta: { filename },
          params: fileStream,
        } = ctx;

        const gdi = new GeospatialDatasetIntegrator();
        const id = await gdi.receiveDataset(
          <string>filename,
          <Readable>fileStream
        );

        return { id };
      },
    },

    getGeospatialDatasetLayerNames: {
      visibility: "published",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { id },
        } = ctx;

        const gdi = new GeospatialDatasetIntegrator(id);

        // @ts-ignore
        const layerNameToId = gdi.layerNameToId;
        const layerNames = Object.keys(layerNameToId);

        return layerNames;
      },
    },

    getTableDescriptor: {
      visibility: "published",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { id, layerName },
        } = ctx;

        const gdi = new GeospatialDatasetIntegrator(id);

        const tableDescriptor = await gdi.getLayerTableDescriptor(layerName);

        return tableDescriptor;
      },
    },

    updateTableDescriptor: {
      visibility: "published",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params,
        } = ctx;

        // console.log(inspect(params));

        // @ts-ignore
        const { id } = params;

        const gdi = new GeospatialDatasetIntegrator(id);

        // @ts-ignore
        const result = await gdi.persistLayerTableDescriptor(params);

        return result;
      },
    },

    loadDatabaseTable: {
      visibility: "published",

      async handler(ctx: Context) {
        const {
          // @ts-ignore
          params: { id, layerName },
        } = ctx;

        const gdi = new GeospatialDatasetIntegrator(id);

        const result = await gdi.loadTable(layerName);

        return result;
      },
    },
  },
};
