// import { createReadStream } from "fs";
// import { join } from "path";

// import proxy from "express-http-proxy";

import { Service, ServiceBroker } from "moleculer";
import { isFSA } from "flux-standard-action";
import ApiGateway from "moleculer-web";

// https://github.com/moleculerjs/moleculer-web/blob/master/index.d.ts
type IncomingRequest = typeof ApiGateway.IncomingRequest;
type GatewayResponse = typeof ApiGateway.GatewayResponse;

export default class ApiService extends Service {
  public constructor(broker: ServiceBroker) {
    super(broker);
    // @ts-ignore
    this.parseServiceSchema({
      name: "api",
      mixins: [ApiGateway],
      // More info about settings: https://moleculer.services/docs/0.14/moleculer-web.html
      settings: {
        cors: {
          methods: ["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"],
          origin: "*",
        },

        port: process.env.PORT || 3369,

        routes: [
          /*
            // FIXME: http://localhost:3369/admin/ui results in the following error
            //        {"name":"MoleculerError","message":"res.status is not a function","code":500}
            {
              path: "/admin/ui",

              use: [proxy("http://localhost:3000")],
            },
          */

          {
            //  https://moleculer.services/docs/0.14/moleculer-web.html#Aliases
            //
            //  > There are some internal pointer in req & res objects:
            //  >
            //  >   * req.$ctx are pointed to request context.
            //  >   * req.$service & res.$service are pointed to this service instance.
            //  >   * req.$route & res.$route are pointed to the resolved route definition.
            //  >   * req.$params is pointed to the resolved parameters (from query string & post body)
            //  >   * req.$alias is pointed to the resolved alias definition.
            //  >   * req.$action is pointed to the resolved action.
            //  >   * req.$endpoint is pointed to the resolved action endpoint.
            //  >   * req.$next is pointed to the next() handler if the request comes from ExpressJS.
            //  >
            //  >   E.g.: To access the broker, use req.$service.broker

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
              async "OPTIONS admin/dispatch"(
                req: IncomingRequest,
                res: GatewayResponse
              ) {
                res.end();
              },

              async "POST admin/dispatch"(
                req: IncomingRequest,
                res: GatewayResponse
              ) {
                // TODO TODO TODO Auth and put user info in event meta TODO TODO TODO

                const event = req.$params;

                console.log("foo".repeat(100));
                console.error(JSON.stringify(event, null, 4));

                if (!isFSA(event)) {
                  return next(
                    new Error(
                      "admin/dispatch request body MUST be a FluxStandardAction."
                    )
                  );
                }

                // @ts-ignore
                let { meta: { event_id = null, etl_context_id = null } = {} } =
                  event;

                if (etl_context_id === null) {
                  etl_context_id = await this.broker.call(
                    "dama_dispatcher.spawnDamaContext"
                  );

                  // @ts-ignore
                  event.meta.etl_context_id = etl_context_id;
                }

                const damaaEvent = await req.$service.broker.call(
                  "dama_dispatcher.dispatch",
                  event
                );

                return res.end(JSON.stringify(damaaEvent));
              },

              async "GET admin/query-events"(
                req: IncomingRequest,
                res: GatewayResponse
              ) {
                // TODO TODO TODO Auth and put user info in event meta TODO TODO TODO

                console.log("==> GET admin/query-events");

                const damaaEvents = await req.$service.broker.call(
                  "dama_dispatcher.queryDamaEvents",
                  req.$params
                );

                return res.end(JSON.stringify(damaaEvents));
              },
            },
          },

          {
            callingOptions: {
              timeout: 0,
            },
            path: "/api",
            whitelist: [
              // AVAIL: We want to block ALL actions.
              //        admin/dispatch MUST be the only exposed interface
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
            mappingPolicy: "all", // Available values: "all", "restrict"

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

      methods: {
        /**
				 * Authenticate the request. It checks the `Authorization` token value in the request header.
				 * Check the token value & resolve the user by the token.
				 * The resolved user will be available in `ctx.meta.user`
				 *
				 * PLEASE NOTE, IT'S JUST AN EXAMPLE IMPLEMENTATION. DO NOT USE IN PRODUCTION!
				 *
				 * @param {Context} ctx
				 * @param {any} route
				 * @param {IncomingRequest} req
				 * @returns {Promise}

				async authenticate (ctx: Context, route: any, req: IncomingRequest): Promise < any >  => {
					// Read the token from header
					const auth = req.headers.authorization;

					if (auth && auth.startsWith("Bearer")) {
						const token = auth.slice(7);

						// Check the token. Tip: call a service which verify the token. E.g. `accounts.resolveToken`
						if (token === "123456") {
							// Returns the resolved user. It will be set to the `ctx.meta.user`
							return {
								id: 1,
								name: "John Doe",
							};

						} else {
							// Invalid token
							throw new ApiGateway.Errors.UnAuthorizedError(ApiGateway.Errors.ERR_INVALID_TOKEN, {
								error: "Invalid Token",
							});
						}

					} else {
						// No token. Throw an error or do nothing if anonymous access is allowed.
						// Throw new E.UnAuthorizedError(E.ERR_NO_TOKEN);
						return null;
					}
				},
				 */
        /**
				 * Authorize the request. Check that the authenticated user has right to access the resource.
				 *
				 * PLEASE NOTE, IT'S JUST AN EXAMPLE IMPLEMENTATION. DO NOT USE IN PRODUCTION!
				 *
				 * @param {Context} ctx
				 * @param {Object} route
				 * @param {IncomingRequest} req
				 * @returns {Promise}

				async authorize (ctx: Context < any, {
					user: string;
				} > , route: Record<string, undefined>, req: IncomingRequest): Promise < any > => {
					// Get the authenticated user.
					const user = ctx.meta.user;

					// It check the `auth` property in action schema.
					// @ts-ignore
					if (req.$action.auth === "required" && !user) {
						throw new ApiGateway.Errors.UnAuthorizedError("NO_RIGHTS", {
							error: "Unauthorized",
						});
					}
				},
				 */
      },
    });
  }
}
