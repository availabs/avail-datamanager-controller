import { Readable } from "stream";

import { Context, Service, ServiceBroker } from "moleculer";
import ApiGateway from "moleculer-web";

// Express middlewares
import compression from "compression";
import serveStatic from "serve-static";

import _ from "lodash";

import pgEnvs from "../../var/pgEnvs";
import dataDir from "../../constants/dataDir";

// import enhanceNCEI from "../dama_integration/actions/ncei_storm_events/postUploadProcessData";
// import openFemaDataLoader from "../dama_integration/actions/openFemaData/openFemaDataLoader";

// https://github.com/moleculerjs/moleculer-web/blob/master/index.d.ts
type IncomingRequest = typeof ApiGateway.IncomingRequest;
type GatewayResponse = typeof ApiGateway.GatewayResponse;

export default class ApiService extends Service {
  public constructor(broker: ServiceBroker) {
    super(broker);
    // @ts-ignore
    this.parseServiceSchema({
      name: "dama_api_server",
      mixins: [ApiGateway],
      // More info about settings: https://moleculer.services/docs/0.14/moleculer-web.html
      settings: {
        cors: {
          methods: ["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"],
          origin: "*",
        },

        port: process.env.PORT,

        routes: [
          // Serve the files in data/
          {
            path: "/files/",
            use: [compression(), serveStatic(dataDir)],
          },
          {
            path: "/dama-info/",

            aliases: {
              async "GET list-postgres-environments"(
                _req: IncomingRequest,
                res: GatewayResponse
              ) {
                return res.end(JSON.stringify(pgEnvs));
              },

              "GET getTileServerUrl":
                "dama/tilerserver-controller.getTileServerUrl",

              "GET getTileServerHealthStatus":
                "dama/tilerserver-controller.getServerHealthStatus",
            },

            bodyParsers: {
              json: {
                strict: false,
                limit: "100MB",
              },
              urlencoded: {
                extended: true,
                limit: "100MB",
              },
            },
          },

          /*
          // FIXME: Cannot get proxying to work.
          // import proxy from "express-http-proxy";
          {
            path: "/dama-tiles",

            use: proxy(`127.0.0.1:${process.env.TILESERVER_PORT}`),
          },
          */

          {
            path: "/dama-admin/:pgEnv",

            onBeforeCall(ctx: Context) {
              // @ts-ignore
              const pgEnv = ctx.params.req.$params.pgEnv;

              if (!pgEnvs.includes(pgEnv)) {
                throw new Error(`Unsupported Postgres environment: ${pgEnv}`);
              }

              // @ts-ignore
              ctx.meta.pgEnv = pgEnv;
            },

            bodyParsers: {
              json: {
                strict: false,
                limit: "100MB",
              },
              urlencoded: {
                extended: true,
                limit: "100MB",
              },
            },

            //  https://moleculer.services/docs/0.14/moleculer-web.html#Aliases
            aliases: {
              // NOTE: Requires an etlContextId query parameter: ?etlContextId=n
              "/getEtlProcessFinalEvent":
                "data_manager/events.queryEtlContextFinalEvent",

              async "GET table-json-schema"(
                req: IncomingRequest,
                res: GatewayResponse
              ) {
                const schema = await req.$ctx.call(
                  "dama/metadata.getTableJsonSchema",
                  req.$params
                );

                res.end(JSON.stringify(schema));
              },

              async "GET table-columns"(
                req: IncomingRequest,
                res: GatewayResponse
              ) {
                const columns = await req.$ctx.call(
                  "dama/metadata.getTableColumns",
                  req.$params
                );

                res.end(JSON.stringify(columns));
              },

              "GET dama-data-sources": "dama/metadata.getDamaDataSources",

              // No idea why this isnt' working
              // "GET events/query": "data_manager/events.queryEvents",
              //
              async "GET events/query"(
                req: IncomingRequest,
                res: GatewayResponse
              ) {
                // TODO TODO TODO Auth and put user info in event meta TODO TODO TODO

                const damaaEvents = await req.$ctx.call(
                  "data_manager/events.queryEvents",
                  req.$params
                );

                return res.end(JSON.stringify(damaaEvents));
              },

              async "gis/dama-view-geojsonl/:damaViewId"(
                req: IncomingRequest,
                res: GatewayResponse
              ) {
                const {
                  $params: { damaViewId },
                } = req;

                res.setHeader(
                  "Content-Type",
                  "application/json; charset=utf-8"
                );

                res.setHeader(
                  "Content-Disposition",
                  `attachment; filename="gis-dataset-${damaViewId}.geojsonl"`
                );

                const stream: Readable = await req.$ctx.call(
                  "dama/gis.makeDamaGisDatasetViewGeoJsonlStream",
                  { damaViewId }
                );

                return stream.pipe(res);
              },

              // Immediately create
              createNewDamaSource: "dama/metadata.createNewDamaSource",

              createNewDamaView: "dama/metadata.createNewDamaView",

              deleteDamaView: "dama/metadata.deleteDamaView",

              deleteDamaSource: "dama/metadata.deleteDamaSource",

              makeAuthoritativeDamaView:
                "dama/metadata.makeAuthoritativeDamaView",

              "GET metadata/datasource-latest-view-table-columns":
                "dama/metadata.getDataSourceLatestViewTableColumns",

              // ETL
              "GET /etl/new-context-id": "data_manager/events.spawnEtlContext",

              // --- gis data set --//

              "gis-dataset/upload": "multipart:gis-dataset.uploadFile",

              "GET /gis-dataset/:id/layerNames": "gis-dataset.getLayerNames",

              "GET /gis-dataset/:id/:layerName/tableDescriptor":
                "gis-dataset.getTableDescriptor",

              "GET /gis-dataset/:id/:layerName/layerAnalysis":
                "gis-dataset.getLayerAnalysis",

              "gis-dataset/publish": "gis-dataset.publish",
              "gis-dataset/createDownload": "gis-dataset.createDownload",


              "gis-dataset/getTaskFinalEvent/:etlContextId": "gis-dataset.getTaskFinalEvent",

              // --- end data set --//
              "gis/create-mbtiles/damaViewId/:damaViewId":
                "dama/gis.createDamaGisDatasetViewMbtiles",

              // FIXME: Returns an array. See
              // https://github.com/moleculerjs/moleculer-web/blob/5b0eebe83ece78dbacd40d02ae90fd7c143572ed/src/alias.js#L194
              "staged-geospatial-dataset/uploadGeospatialDataset":
                "multipart:dama/data_source_integrator.uploadGeospatialDataset",

              "GET /staged-geospatial-dataset/:id/layerNames":
                "dama/data_source_integrator.getGeospatialDatasetLayerNames",

              "GET /staged-geospatial-dataset/:id/:layerName/tableDescriptor":
                "dama/data_source_integrator.getTableDescriptor",

              "GET /staged-geospatial-dataset/:id/:layerName/layerAnalysis":
                "dama/data_source_integrator.getLayerAnalysis",

              "staged-geospatial-dataset/stageLayerData/:layerName":
                "dama/data_source_integrator.stageLayerData",

              "/staged-geospatial-dataset/:id/updateTableDescriptor":
                "dama/data_source_integrator.updateTableDescriptor",

              "/staged-geospatial-dataset/:id/:layerName/loadDatabaseTable":
                "dama/data_source_integrator.loadDatabaseTable",

              "POST /staged-geospatial-dataset/dispatchCreateDamaSourceEvent":
                "dama/data_source_integrator.dispatchCreateDamaSourceEvent",

              "POST /staged-geospatial-dataset/dispatchCreateDamaViewEvent":
                "dama/data_source_integrator.dispatchCreateDamaViewEvent",

              "/staged-geospatial-dataset/approveQA":
                "dama/data_source_integrator.approveQA",

              "/staged-geospatial-dataset/publishGisDatasetLayer":
                "dama/data_source_integrator.publishGisDatasetLayer",

              "/hazard_mitigation/hlrLoader":
                "hazard_mitigation.hlrLoader.load",

              "/hazard_mitigation/ealLoader":
                "hazard_mitigation.ealLoader.load",

              "/hazard_mitigation/loadNCEI": "hazard_mitigation.loadNCEI.load",

              "/hazard_mitigation/enhanceNCEI":
                "hazard_mitigation.enhancedNCEI.load",

              "/hazard_mitigation/zoneToCountyLoader":
                "hazard_mitigation.zoneToCountyLoader.load",

              "/hazard_mitigation/tigerDownloadAction":
                "hazard_mitigation.tigerDownloadAction.load",

              "/hazard_mitigation/tigerFullDownloadAction":
                "hazard_mitigation.tigerFullDownloadAction.load",

              "/hazard_mitigation/versionSelectorUtils":
                "hazard_mitigation.versionSelectorUtils.load",

              "/hazard_mitigation/cacheAcs":
                "hazard_mitigation.cacheAcs.load",

              "/hazard_mitigation/disaster_declarations_summary_v2":
                "hazard_mitigation.disaster_declarations_summary_v2.load",

              "/hazard_mitigation/ihp_v1": "hazard_mitigation.ihp_v1.load",

              "/hazard_mitigation/pa_v1": "hazard_mitigation.pa_v1.load",

              "/hazard_mitigation/hmgp_summaries_v2": "hazard_mitigation.hmgp_summaries_v2.load",

              "/hazard_mitigation/hmgp_properties_v2": "hazard_mitigation.hmgp_properties_v2.load",

              "/hazard_mitigation/hmgp_projects_v2": "hazard_mitigation.hmgp_projects_v2.load",

              "/hazard_mitigation/nfip_v1": "hazard_mitigation.nfip_v1.load",

              "/hazard_mitigation/nfip_v1_enhanced":
                "hazard_mitigation.nfip_v1_enhanced.load",

              "/hazard_mitigation/usdaLoader":
                "hazard_mitigation.usdaLoader.load",

              "/hazard_mitigation/usda_enhanced":
                "hazard_mitigation.usda_enhanced.load",

              "/hazard_mitigation/sbaLoader":
                "hazard_mitigation.sbaLoader.load",

              "/hazard_mitigation/nriLoader":
                "hazard_mitigation.nriLoader.load",

              "/hazard_mitigation/nriTractsLoader":
                "hazard_mitigation.nriTractsLoader.load",

              "/hazard_mitigation/pbSWDLoader":
                "hazard_mitigation.pbSWDLoader.load",

              "/hazard_mitigation/pbFusionLoader":
                "hazard_mitigation.pbFusionLoader.load",

              "/hazard_mitigation/disasterLossSummaryLoader":
                "hazard_mitigation.disasterLossSummaryLoader.load",

              "/hazard_mitigation/fusionLoader":
                "hazard_mitigation.fusionLoader.load",

              "data-types/npmrds/getToposortedDamaSourcesMeta":
                "dama/data_types/npmrds.getToposortedDamaSourcesMeta",

              "data-types/npmrds/initializeNpmrdsSources":
                "dama/data_types/npmrds.initializeDamaSources",

              "/data-types/npmrds/npmrds-travel-times-export-ritis/getNpmrdsDataDateExtent":
                "dama/data_types/npmrds/dt-npmrds_travel_times_export_ritis.getNpmrdsDataDateExtent",

              "/data-types/npmrds/npmrds-travel-times-export-ritis/queueNpmrdsExportRequest":
                "dama/data_types/npmrds/dt-npmrds_travel_times_export_ritis.queueNpmrdsExportRequest",

              "/data-types/npmrds/npmrds-travel-times-export-ritis/getOpenRequestsStatuses":
                "dama/data_types/npmrds/dt-npmrds_travel_times_export_ritis.getOpenRequestsStatuses",

              "/data-types/npmrds/npmrds-travel-times/makeTravelTimesExportTablesAuthoritative":
                "dama/data_types/npmrds/dt-npmrds_travel_times.makeTravelTimesExportTablesAuthoritative",

              "POST /data-types/npmrds/network-analysis/getTmcs":
                "dama/data_types/npmrds/network-analysis.getTmcs",

              "POST /data-types/npmrds/network-analysis/getTmcFeatures":
                "dama/data_types/npmrds/network-analysis.getTmcFeatures",
            },
          },

          {
            // Calling options. More info: https://moleculer.services/docs/0.14/moleculer-web.html#Calling-options
            callingOptions: {
              timeout: 0,
            },
            path: "/api",
            whitelist: [
              // // Access to any actions in all services under "/api" URL
              // "admin/api",
              "**",
            ],
            //  Route-level Express middlewares.
            //    More info: https://moleculer.services/docs/0.14/moleculer-web.html#Middlewares
            use: [],
            //  Enable/disable parameter merging method.
            //    More info: https://moleculer.services/docs/0.14/moleculer-web.html#Disable-merging
            mergeParams: true,

            //  Enable authentication. Implement the logic into `authenticate` method.
            //    More info: https://moleculer.services/docs/0.14/moleculer-web.html#Authentication
            authentication: false,

            //  Enable authorization. Implement the logic into `authorize` method.
            //    More info: https://moleculer.services/docs/0.14/moleculer-web.html#Authorization
            authorization: false,

            //  The auto-alias feature allows you to declare your route alias directly in your services.
            //  The gateway will dynamically build the full routes from service schema.
            autoAliases: true,

            aliases: {},
            /**
					 * Before call hook. You can check the request.
					 * @param {Context} ctx
					 * @param {Object} route
					 * @param {IncomingRequest} req
					 * @param {GatewayResponse} res
					 * @param {Object} data
					onBeforeCall(ctx: Context<any,{userAgent: string}>,
					 route: object, req: IncomingRequest, res: GatewayResponse) {
					  Set request headers to context meta
					  ctx.meta.userAgent = req.headers["user-agent"];
					},
					 */

            /**
					 * After call hook. You can modify the data.
					 * @param {Context} ctx
					 * @param {Object} route
					 * @param {IncomingRequest} req
					 * @param {GatewayResponse} res
					 * @param {Object} data
					 *
					 onAfterCall(ctx: Context, route: object, req: IncomingRequest, res: GatewayResponse, data: object) {
					// Async function which return with Promise
					return doSomething(ctx, res, data);
				},
					 */

            bodyParsers: {
              json: {
                strict: false,
                limit: "100MB",
              },
              urlencoded: {
                extended: true,
                limit: "100MB",
              },
            },

            // Mapping policy setting. More info: https://moleculer.services/docs/0.14/moleculer-web.html#Mapping-policy
            mappingPolicy: "restrict", // Available values: "all", "restrict"

            // Enable/disable logging
            logging: true,
          },
        ],
        // Do not log client side errors (does not log an error response when the error.code is 400<=X<500)
        log4XXResponses: false,
        // Logging the request parameters. Set to any log level to enable it. E.g. "info"
        logRequestParams: null,
        // Logging the response data. Set to any log level to enable it. E.g. "info"
        logResponseData: null,
        // Serve assets from "public" folder
        assets: {
          folder: "public",
          // Options to `server-static` module
          options: {},
        },
      },

      methods: {},
    });
  }
}
