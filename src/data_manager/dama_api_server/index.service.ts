import { Context, Service, ServiceBroker } from "moleculer";
import { isFSA } from "flux-standard-action";
import ApiGateway from "moleculer-web";

import _ from "lodash";

import pgEnvs from "../../var/pgEnvs";

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
          {
            path: "/dama-info/",

            aliases: {
              async "GET list-postgres-environments"(
                _req: IncomingRequest,
                res: GatewayResponse
              ) {
                return res.end(JSON.stringify(pgEnvs));
              },
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
              /* For when the CRA is built and served static.
                "GET admin/ui"(_req, res) {
                  if (process.env.NODE_ENV === "production") {
                    const fpath = join(__dirname, "./admin-ui.html");
                    const rs = createReadStream(fpath);

                    return rs.pipe(res);
                  }
                },
              */
              // async "OPTIONS events/dispatch"(
              // _req: IncomingRequest,
              // res: GatewayResponse
              // ) {
              // res.end();
              // },

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

              async "GET dama-data-sources"(
                req: IncomingRequest,
                res: GatewayResponse
              ) {
                const dataSources = await req.$ctx.call(
                  "dama/metadata.getDamaDataSources",
                  req.$params
                );

                res.end(JSON.stringify(dataSources));
              },

              async "events/dispatch"(
                req: IncomingRequest,
                res: GatewayResponse
              ) {
                // TODO TODO TODO Auth and put user info in event meta TODO TODO TODO

                const event = _.omit(req.$params, "pgEnv");

                // @ts-ignore
                const { pgEnv } = req.$ctx.meta;

                event.meta = event.meta || {};
                event.meta.pgEnv = pgEnv;

                let { meta: { etl_context_id = null } = {} } = event;

                if (etl_context_id === null) {
                  etl_context_id = await req.$ctx.call(
                    "dama_dispatcher.spawnDamaContext"
                  );

                  // @ts-ignore
                  event.meta.etl_context_id = etl_context_id;
                }

                const damaaEvent = await req.$ctx.call(
                  "dama_dispatcher.dispatch",
                  event
                );

                return res.end(JSON.stringify(damaaEvent));
              },

              async "GET events/query"(
                req: IncomingRequest,
                res: GatewayResponse
              ) {
                // TODO TODO TODO Auth and put user info in event meta TODO TODO TODO

                const damaaEvents = await req.$ctx.call(
                  "dama_dispatcher.queryDamaEvents",
                  req.$params
                );

                return res.end(JSON.stringify(damaaEvents));
              },

              "POST metadata/createNewDataSource":
                "dama/metadata.createNewDataSource",

              "GET metadata/datasource-latest-view-table-columns":
                "dama/metadata.getDataSourceLatestViewTableColumns",

              "GET new-etl-context-id": "dama_dispatcher.spawnDamaContext",

              "GET staged-geospatial-dataset/existingDatasetUploads":
                "dama/data_source_integrator.getExistingDatasetUploads",

              // FIXME: Returns an array. See
              // https://github.com/moleculerjs/moleculer-web/blob/5b0eebe83ece78dbacd40d02ae90fd7c143572ed/src/alias.js#L194
              "staged-geospatial-dataset/uploadGeospatialDataset":
                "multipart:dama/data_source_integrator.uploadGeospatialDataset",

              "GET staged-geospatial-dataset/:id/layerNames":
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

              "/staged-geospatial-dataset/approveQA":
                "dama/data_source_integrator.approveQA",

              "/staged-geospatial-dataset/submitViewMeta":
                "dama/data_source_integrator.submitViewMeta",

              "/staged-geospatial-dataset/publishGisDatasetLayer":
                "dama/data_source_integrator.publishGisDatasetLayer",

              "/staged-geospatial-dataset/testAction":
                "dama/data_source_integrator.testAction",

              "/staged-geospatial-dataset/testDownloadAction":
                "dama/data_source_integrator.testDownloadAction",

              "/staged-geospatial-dataset/testUploadAction":
                "dama/data_source_integrator.testUploadAction",
            },
          },

          {
            callingOptions: {
              timeout: 0,
            },
            path: "/api",
            whitelist: [
              // AVAIL: We want to block ALL actions.
              //        events/dispatch MUST be the only exposed interface
              //
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

            // Calling options. More info: https://moleculer.services/docs/0.14/moleculer-web.html#Calling-options
            callingOptions: {},

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
