import { ChildProcess, fork } from "child_process";
import { existsSync } from "fs";
import { rename as renameAsync, rm as rmAsync } from "fs/promises";
import { join, basename } from "path";

import { Graph, alg as GraphAlgorithms } from "graphlib";

import dedent from "dedent";

import { Context } from "moleculer";

import _ from "lodash";

import { FSA } from "flux-standard-action";

import { createNpmrdsDataRangeDownloadRequest } from "../../../../tasks/avail-datasources-watcher/src/utils/NpmrdsDataDownloadNames";

import { stateAbbr2FipsCode } from "../../../data_utils/constants/stateFipsCodes";

import damaHost from "../../../constants/damaHost";
import controllerDataDir from "../../../constants/dataDir";

import { NpmrdsDataSources } from "../domain";

import npmrdsTravelTimesExportDataDir from "./constants/dataDir";

const npmrdsDownloadServiceMainPath = join(
  __dirname,
  "../../../../tasks/avail-datasources-watcher/src/dataSources/RITIS/services/NpmrdsDownloadService/",
  process.env.NODE_ENV?.toLowerCase() === "production" ? "main" : "mock"
);

const loadDownloadedExportsIntoSqlitePath = join(
  __dirname,
  "../../../../tasks/avail-datasources-watcher/tasks/downloadedExportsIntoSqlite/run"
);

const initDamaSourceSqlPath = join(
  __dirname,
  "./sql/initialize_npmrds_export_dama_sources.sql"
);

export const serviceName = "dama/data_sources/npmrds/travel_times_export/etl";

const IDLE_TIMEOUT = 1000 * 60 * 60; // One Hour

export default {
  name: serviceName,

  methods: {
    async queueNpmrdsExportRequest(npmrdsDownloadRequest: any) {
      console.log("dama queueNpmrdsExportRequest");
      const srv = await this.getNpmrdsDownloadService();
      console.log("dama gotService");

      srv.send({
        type: "NpmrdsDownloadService:QUEUE_NPMRDS_DOWNLOAD_REQUEST",
        payload: npmrdsDownloadRequest,
      });
      console.log("dama sentReq");
    },

    async stopNpmrdsDownloadService() {
      console.log("stopNpmrdsDownloadService");

      const downloadService = await this._downloadServiceCP;
      const stillRunning = downloadService && downloadService.exitCode !== null;

      if (stillRunning) {
        // TODO: Check return value and handle problem cases
        downloadService.kill();
      }

      this._downloadServiceCP = null;
    },

    resetIdleShutdownTimer() {
      clearTimeout(this._idleShutdownTimer);

      //  https://nodejs.org/docs/latest-v16.x/api/child_process.html#subprocesskillsignal
      //  If the NpmrdsDownloadService is idle for IDLE_TIMEOUT, shut it down.
      //    This periodically refreshes the _npmrds_data_date_extent.
      this._idleShutdownTimer = setTimeout(
        this.stopNpmrdsDownloadService.bind(this),
        IDLE_TIMEOUT
      );
    },

    async getNpmrdsDownloadService() {
      if (!existsSync(npmrdsDownloadServiceMainPath)) {
        throw new Error("NPMRDS Download Service repository not found.");
      }

      // We always restart the timer
      this.resetIdleShutdownTimer();

      const downloadService = await this._downloadServiceCP;
      const isRunning = downloadService && downloadService.exitCode === null;

      if (!isRunning) {
        clearTimeout(this._idleShutdownTimer);

        this._downloadServiceCP = new Promise<ChildProcess>(
          (resolve, reject) => {
            const cp = fork(npmrdsDownloadServiceMainPath)
              // @ts-ignore
              .on("spawn", () => {
                console.log(
                  "\nNpmrdsDownloadService spawned with PID:",
                  cp.pid,
                  "\n"
                );
              })
              // @ts-ignore
              .on("error", reject)
              .on("exit", (code, signal) => {
                if (code) {
                  console.warn(
                    `The NpmrdsDownloadService exited with code ${code}`
                  );
                }

                if (signal) {
                  console.warn(
                    `The NpmrdsDownloadService terminated due to signal ${signal}`
                  );
                }
              })
              .on("message", (event: FSA) => {
                console.log(
                  JSON.stringify({ npmrdsDownloadServiceEvent: event }, null, 4)
                );

                // KEEPALIVE events will resetIdleShutdownTimer
                this.resetIdleShutdownTimer();

                // The NpmrdsDownloadService.READY event payload is the available date extent.
                if (event.type === "NpmrdsDownloadService:READY") {
                  // @ts-ignore
                  const extent: {
                    year: number;
                    month: number;
                    date: number;
                  }[] = event.payload;

                  const [
                    { year: minYear, month: minM, date: minD },
                    { year: maxYear, month: maxM, date: maxD },
                  ] = extent;

                  const minMM = `0${minM}`.slice(-2);
                  const minDD = `0${minD}`.slice(-2);

                  const maxMM = `0${maxM}`.slice(-2);
                  const maxDD = `0${maxD}`.slice(-2);

                  const minDate = `${minYear}-${minMM}-${minDD}`;
                  const maxDate = `${maxYear}-${maxMM}-${maxDD}`;

                  this._npmrds_data_date_extent = [minDate, maxDate];

                  resolve(cp);
                  this.resetIdleShutdownTimer();
                } else {
                  this.handleNpmrdsDownloadServiceEvent(event);
                }
              });
          }
        );
      }

      return await this._downloadServiceCP;
    },

    async handleNpmrdsDownloadServiceEvent(event: FSA) {
      // @ts-ignore
      const { type, meta: { pgEnv, etl_context_id } = {} } = event;

      if (!(pgEnv && etl_context_id)) {
        return;
      }

      if (type === "NpmrdsDownloadService:FINAL") {
        return await this.handleNpmrdsTravelTimesExportDownloaded(event);
      }

      //  We just prefixing the type with this serviceName and
      //    forward STATUS_UPDATEs along to the event store
      //    so that they can be queried and displayed in the admin client.
      if (/STATUS_UPDATE$/.test(type)) {
        const opts = {
          meta: {
            pgEnv,
          },
        };

        const newEvent = {
          ...event,
          type: `${serviceName}/${type}`,
        };

        return await this.broker.call(
          "dama_dispatcher.dispatch",
          newEvent,
          opts
        );
      }
    },

    async moveTransformedDataToCanonicalDirectory(transformDoneData: any) {
      const { npmrdsTravelTimesExport: oldNpmrdsTravelTimesExport } =
        transformDoneData;

      const npmrdsTravelTimesExportBase = basename(oldNpmrdsTravelTimesExport);

      const newNpmrdsTravelTimesExport = join(
        npmrdsTravelTimesExportDataDir,
        npmrdsTravelTimesExportBase
      );

      if (existsSync(newNpmrdsTravelTimesExport)) {
        if (process.env?.NODE_ENV?.toLowerCase() === "development") {
          console.warn(
            "\nBecause NODE_ENV === development, removing existing NpmrdsTravelTimesExport directory.\n"
          );
          await rmAsync(newNpmrdsTravelTimesExport, {
            recursive: true,
            force: true,
          });
        } else {
          throw Error(
            `NpmrdsTravelTimesExport directory already exists: ${newNpmrdsTravelTimesExport}`
          );
        }
      }

      await renameAsync(oldNpmrdsTravelTimesExport, newNpmrdsTravelTimesExport);

      const exportRootRelativePath = newNpmrdsTravelTimesExport.replace(
        controllerDataDir,
        ""
      );

      const doneData = _(transformDoneData)
        .mapKeys((_v, k: string) => k.charAt(0).toUpperCase() + k.slice(1))
        .mapValues((v, k) => {
          // Can't download the directory, only files.
          // TODO:  use fs.stat to determine if the path points to a file or directory.
          //        SEE: https://nodejs.org/api/fs.html#fsstatpath-options-callback
          const path =
            k === NpmrdsDataSources.NpmrdsTravelTimesExport
              ? null
              : v.replace(oldNpmrdsTravelTimesExport, exportRootRelativePath);

          return {
            metadata: {
              filelocation: {
                host: damaHost,
                path,
              },
            },
          };
        })
        .value();

      return doneData;
    },

    async handleNpmrdsTravelTimesExportDownloaded(event: FSA) {
      // @ts-ignore
      const { payload, meta: { pgEnv, etl_context_id } = {} } = event;

      if (!(pgEnv && etl_context_id)) {
        return;
      }

      // @ts-ignore
      const { npmrdsDownloadName } = payload;

      const opts = {
        meta: {
          pgEnv,
        },
      };

      // FIXME: Need to emit the DamaView info into the event_store
      const downloadedEvent = {
        ...event,
        type: `${serviceName}:NPMRDS_TRAVEL_TIMES_EXPORT_DOWNLOADED`,
      };

      await this.broker.call("dama_dispatcher.dispatch", downloadedEvent, opts);

      const transformEvent = {
        type: `${serviceName}:STATUS_UPDATE`,
        payload: {
          status: "TRANSFORMING",
          // @ts-ignore
          npmrdsDownloadName: event.payload.npmrdsDownloadName,
        },
        meta: event.meta,
      };

      await this.broker.call("dama_dispatcher.dispatch", transformEvent, opts);

      const transformDoneData = await this.transformNpmrdsExportDownload(
        npmrdsDownloadName
      );

      const moveDoneData = await this.moveTransformedDataToCanonicalDirectory(
        transformDoneData
      );

      console.log(JSON.stringify({ transformDoneData }, null, 4));

      // TODO:
      //        1. Move the npmrdsTravelTimesExport dir to the DamaCntlrDataDir
      //        2. Alter all the paths
      //          1. basename of the npmrdsExportsDir
      //          2. move to NpmrdsTravelTimesExport's damaCntrlrDataDir
      //          3. all other paths, simple string replace old npmrdsExportsDir with new npmrdsExportsDir

      const transformDoneEvent = {
        type: `${serviceName}:STATUS_UPDATE`,
        payload: {
          status: "TRANSFORM_DONE",
          doneData: moveDoneData,
        },
        meta: event.meta,
      };

      console.log(JSON.stringify({ transformDoneEvent }, null, 4));
      await this.broker.call(
        "dama_dispatcher.dispatch",
        transformDoneEvent,
        opts
      );

      // LOAD
      //
      const {
        [NpmrdsDataSources.NpmrdsTravelTimesExportSqlite]: {
          metadata: {
            filelocation: { path: npmrdsTravelTimesSqliteDbPath },
          },
        },
      } = moveDoneData;

      const npmrdsExportSqliteDbPath = join(
        controllerDataDir,
        npmrdsTravelTimesSqliteDbPath
      );

      const loadDoneData = await this.broker.call(
        "dama/data_sources/npmrds/travel_times/db_loader.loadNpmrdsTravelTimes",
        { npmrdsExportSqliteDbPath },
        opts
      );

      const loadDoneEvent = {
        type: `${serviceName}:STATUS_UPDATE`,
        payload: {
          status: "LOAD_DONE",
          doneData: loadDoneData,
        },
        meta: event.meta,
      };

      console.log(JSON.stringify({ loadDoneEvent }, null, 4));
      await this.broker.call("dama_dispatcher.dispatch", loadDoneEvent, opts);

      const doneData = {
        ...moveDoneData,
        [NpmrdsDataSources.NpmrdsTravelTimesExportDb]: { ...loadDoneData },
      };

      const integrateEvent = {
        type: "INTEGRATE_INTO_VIEWS",
        payload: {
          npmrdsDownloadName,
          doneData,
        },
        meta: event.meta,
      };

      const toposortedDamaViewInfo =
        await this.integrateNpmrdsTravelTimesEtlIntoDataManager(integrateEvent);

      const finalEvent = {
        type: `${serviceName}:FINAL`,
        payload: {
          initialData: event.payload || null,
          etlDoneData: doneData,
          damaIntegrationDoneData: toposortedDamaViewInfo,
        },
        meta: event.meta,
      };

      console.log(JSON.stringify({ finalEvent }, null, 4));

      await this.broker.call("dama_dispatcher.dispatch", finalEvent, opts);
    },

    async integrateNpmrdsTravelTimesEtlIntoDataManager(event: FSA) {
      const { payload, meta } = event;
      const {
        // @ts-ignore
        npmrdsDownloadName,
        // @ts-ignore
        doneData: etlDoneData,
      } = payload;

      // @ts-ignore
      const { etl_context_id, user_id } = meta;

      const rootEtlContextId = await this.broker.call(
        "dama_dispatcher.queryRootContextId",
        { etl_context_id },
        { meta }
      );

      const startDate = npmrdsDownloadName
        .replace(/.*_from_/, "")
        .replace(/_to.*/, "");

      const endDate = npmrdsDownloadName
        .replace(/.*_to_/, "")
        .replace(/_.*/, "");

      const {
        state = null,
        year = null,
        data_start_date = null,
        // data_end_date = null,
        is_expanded = null,
        is_complete_month = null,
        download_timestamp = null,
      } = etlDoneData[NpmrdsDataSources.NpmrdsTravelTimesExportDb]?.metadata ||
      {};

      const stateFips = is_expanded ? stateAbbr2FipsCode[state] : null;
      const intervalVersion =
        (is_complete_month &&
          data_start_date?.replace(/[^0-9]/g, "").slice(0, 6)) ||
        null;

      const sql = dedent(`
        WITH cte_deps AS (
          SELECT
              array_agg(view_id ORDER BY view_id) AS deps
            FROM data_manager.views
            WHERE (
              ( etl_context_id = $1 )         -- $1
              AND
              ( source_id = ANY( $2 ) )       -- $2
            )
        )
          INSERT INTO data_manager.views (
                                              -- columnValues from function variables
            etl_context_id,                   -- $1
            source_id,                        -- $3
            version,                          -- $4
            start_date,                       -- $5
            end_date,                         -- $6
            last_updated,                     -- $7
            interval_version,                 -- $8
            geography_version,                -- $9
            user_id,                          -- $10
            root_etl_context_id,              -- $11
                                              -- column values from EtlDoneData
            table_schema,                     -- $12
            table_name,                       -- $13
            metadata,                         -- $14
            statistics,                       -- $15
            view_dependencies                 -- from cte_deps
          ) VALUES (
            $1,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8,
            $9,
            $10,
            $11,
            $12,
            $13,
            $14,
            $15,
            ( SELECT deps FROM cte_deps )
          ) RETURNING *
        ;
      `);

      const columnValuesFromEtlDoneData = [
        "table_schema",
        "table_name",
        "metadata",
        "statistics",
      ];

      const toposortedSourceInfo = await this.broker.call(
        `${serviceName}.initializeNpmrdsExportDamaSources`,
        {}, // FIXME: Why is this needed? Was getting error about trying to deref { etl_context_id }
        { meta }
      );

      const stmts: any[] = ["BEGIN;"];

      for (const {
        name,
        source_id,
        source_dependencies,
      } of toposortedSourceInfo) {
        console.log("==>", name);
        const damaSrcDoneData = etlDoneData[name];

        if (!damaSrcDoneData) {
          continue;
        }

        const [_startDate, _endDate] =
          name === NpmrdsDataSources.NpmrdsTmcIdentificationCsv
            ? [`${year}-01-01`, `${year}-12-31`]
            : [startDate, endDate];

        const values = [
          etl_context_id,
          source_dependencies,
          source_id,
          npmrdsDownloadName,
          _startDate,
          _endDate,
          download_timestamp,
          intervalVersion, // TODO : yrmo: Null if not complete month. Check for making authoritative.
          stateFips,
          user_id,
          rootEtlContextId,
        ];

        columnValuesFromEtlDoneData.forEach((col) => {
          const v = damaSrcDoneData[col];

          values.push(v === undefined ? null : v);
        });

        stmts.push({ text: sql, values });
      }

      stmts.push("COMMIT");

      const result = await this.broker.call("dama_db.query", stmts, { meta });

      const viewInfoBySourceId = result.reduce((acc, res) => {
        const {
          rows: [row],
        } = res;
        if (row) {
          const { source_id } = row;
          acc[source_id] = row;
        }
        return acc;
      }, {});

      const toposortedDamaViewInfo = toposortedSourceInfo
        .map((d: any) => {
          const viewInfo = viewInfoBySourceId[d.source_id];

          if (!viewInfo) {
            return null;
          }

          return { ...d, ...viewInfo };
        })
        .filter(Boolean);

      return toposortedDamaViewInfo;
    },

    async transformNpmrdsExportDownload(npmrdsDownloadName: string) {
      const transformDoneData = await new Promise((resolve, reject) =>
        fork(
          loadDownloadedExportsIntoSqlitePath,
          ["--npmrdsDownloadName", npmrdsDownloadName],
          { stdio: "inherit" }
        )
          .on("spawn", () => {
            console.log(
              "spawned a transformNpmrdsExportDownload for",
              npmrdsDownloadName
            );
          })
          // @ts-ignore
          .on("error", reject)
          .on("exit", (code, signal) => {
            if (code) {
              reject(
                new Error(
                  `The transformNpmrdsExportDownload subprocess exited with code ${code}`
                )
              );
            }

            if (signal) {
              reject(
                new Error(
                  `The transformNpmrdsExportDownload subprocess terminated due to signal ${signal}`
                )
              );
            }
          })
          .on("message", (event: FSA) => {
            const { type, payload } = event;
            if (type === "downloadedExportsIntoSqlite:FINAL") {
              resolve(payload);
            }
          })
      );

      return transformDoneData;
    },
  },

  actions: {
    async initializeNpmrdsExportDamaSources(ctx: Context) {
      await ctx.call("dama_db.executeSqlFile", {
        sqlFilePath: initDamaSourceSqlPath,
      });

      const sql = dedent(`
        WITH RECURSIVE cte_deps_tree(source_id) AS (
          SELECT
              source_id
            FROM data_manager.sources
            WHERE ( name = $1 )
          UNION    
          SELECT
              a.source_id
            FROM data_manager.sources AS a
              INNER JOIN cte_deps_tree AS b
                ON ( b.source_id = ANY(a.source_dependencies) )
        )
          SELECT
              source_id,
              name,
              source_dependencies
            FROM data_manager.sources
              INNER JOIN cte_deps_tree
                USING (source_id)
      `);

      const {
        rows,
      }: {
        rows: {
          source_id: number;
          name: string;
          source_dependencies: null | string[];
        }[];
      } = await ctx.call("dama_db.query", {
        text: sql,
        values: [NpmrdsDataSources.NpmrdsTravelTimesExport],
      });

      const sourcesById = rows.reduce((acc, row) => {
        acc[row.source_id] = row;
        return acc;
      }, {});

      const g = new Graph({
        directed: true,
        multigraph: false,
        compound: false,
      });

      for (const { source_id, source_dependencies } of rows) {
        if (source_dependencies === null) {
          g.setNode(`${source_id}`);
          continue;
        }

        // We flatten the depencencies because NpmrdsAuthoritativeTravelTimesDb's is 2-dimensional.
        for (const id of _.flattenDeep(source_dependencies) || []) {
          if (+id !== +source_id) {
            g.setEdge(`${id}`, `${source_id}`);
          }
        }
      }

      const toposortedSourceIds = GraphAlgorithms.topsort(g);

      const toposortedSourceInfo = toposortedSourceIds.map(
        (id) => sourcesById[id]
      );

      console.log();
      console.log();
      console.log("=== initializeNpmrdsExportDamaSources ===");
      console.log(JSON.stringify({ toposortedSourceInfo }, null, 4));
      console.log();
      console.log();
      console.log();

      return toposortedSourceInfo;
    },

    // x. get an EtlContextId and add to req etl_context
    // x. emit to db.event_store
    // x. submit download req
    // x. service update listener should emit queued and send to event_store
    // 5. after downloaded
    //    1. publish raw RITIS schema CSVs in data_manager.views
    //    2. transform to SQLite should kick off
    // 6. after transformed, publish HERE schema CSV in data_manager.views
    async queueNpmrdsExportRequest(ctx: Context) {
      const {
        // @ts-ignore
        params: { state, start_date, end_date, is_expanded = true, user_id },
        // @ts-ignore
        meta: { pgEnv },
      } = ctx;

      const etl_context_id = await ctx.call("dama_dispatcher.spawnDamaContext");
      const dama_controller_host = damaHost;

      const initialEvent = {
        type: `${serviceName}:INITIAL`,
        payload: {
          state,
          start_date,
          end_date,
          is_expanded,
        },
        meta: {
          dama_controller_host,
          etl_context_id,
          user_id,
          timestamp: new Date().toISOString(),
        },
      };

      await ctx.call("dama_dispatcher.dispatch", initialEvent);

      // @ts-ignore
      const req = createNpmrdsDataRangeDownloadRequest({
        state,
        start_date,
        end_date,
        is_expanded,
        // @ts-ignore
        etl_context: { pgEnv, etl_context_id, dama_controller_host },
      });

      console.log(
        "=".repeat(5),
        "Sending NpmrdsDownloadRequest",
        "=".repeat(5)
      );
      console.log(req);

      // NpmrdsDownloadService.queueNpmrdsDownloadRequest(req);
      // NOTE:
      //        NpmrdsDownloadId is not known until QUEUED QUEUE_STATUS_UPDATE event received.
      //        NpmrdsDownloadName is not known until PENDING QUEUE_STATUS_UPDATE event received.
      await this.queueNpmrdsExportRequest(req);

      return req;
    },

    async getNpmrdsDataDateExtent() {
      // May need to (re)start the NpmrdsDownloadService to get/refresh the _npmrds_data_date_extent
      await this.getNpmrdsDownloadService();

      return this._npmrds_data_date_extent;
    },

    testTransformDownloadedNpmrdsExportIntoSqliteDb: {
      visibility: "protected",

      async handler(ctx: Context) {
        return await this.transformNpmrdsExportDownload(
          // @ts-ignore
          ctx.params.npmrdsDownloadName
        );
      },
    },

    async getOpenRequestsStatuses(ctx: Context) {
      // @ts-ignore
      const openRequestStatuses: any[] = await ctx.call(
        "dama_db.queryOpenEtlProcessesStatusUpdatesForService",
        {
          serviceName,
        }
      );

      console.log(JSON.stringify({ openRequestStatuses }, null, 4));

      //  NOTE: The sorted order of the openRequestStatuses is a best-effort representation
      //          of the state of the NpmrdsDownloadRequests Queue.
      //
      //        However, it cannot accurately show the order that requests will complete
      //          for the following reasons:
      //
      //          1.  Queued NpmrdsDownloadRequests are handled serially, then concurrently.
      //
      //          2.  We only make one NpmrdsDownloadRequests from RITIS at a time.
      //                The queue holds back the next export request until
      //                RITIS notifies us that current export request is read to download.
      //
      //          3.  Downloads happen serially, but concurrently with export request.
      //
      //          4.  Transforms happen concurrently with export requests, downloads,
      //                and other transforms.
      //
      //        The sorted order of the openRequestStatuses concerns itself
      //          ONLY with the serial ordering up to sending export requests to RITIS.
      //          Once things get concurrent, there is insufficient information in the event_store
      //            to predict the order in which the downloads and transforms will finish.
      //
      // FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME FIXME
      //    Newly added NpmrdsDownloadRequests seem to immediately jump to the head
      //      of the openRequestStatuses then fall back to the end. Not sure why.
      //
      openRequestStatuses.sort((a: any, b: any) => {
        const {
          etl_context_id: aEtlCtxId,
          payload: {
            queuePriority: aPri = 1,
            insertedAt: aInsertedAt,
            npmrdsDownloadName: aName,
          },
        } = a;

        const {
          etl_context_id: bEtlCtxId,
          payload: {
            queuePriority: bPri = 1,
            insertedAt: bInsertedAt,
            npmrdsDownloadName: bName,
          },
        } = b;

        const aNameTs = aName?.replace(/.*_v/, "");
        const bNameTs = bName?.replace(/.*_v/, "");

        //  npmrdsDownloadName (with its timestamp) is created when the export request is sent to RITIS.
        //    It accurately represents when the data was requested.
        //
        //  Example name: npmrds_vt_from_20211201_to_20211201_v20221230T115229
        if (aNameTs && bNameTs) {
          // chronologically by npmrdsDownloadName's timestamp
          return aNameTs.localeCompare(bNameTs);
        } else if (aNameTs) {
          // a has been requested, b still queued: a preceeds b in the queue
          return -1;
        } else if (bNameTs) {
          // b has been requested, a still queued: b preceeds a in the queue
          return 1;
        }

        // Neither have been assigned a npmrdsDownloadName.
        // They are both awaiting form submission in the NpmrdsDownloadService priority queue.

        // Do their priorities differ? If so, sort descending by priority
        if (aPri !== bPri) {
          return bPri - aPri;
        }

        // sort in ascending order by inserted at timestamp
        const insrtComp =
          aInsertedAt && bInsertedAt
            ? aInsertedAt.localeCompare(bInsertedAt)
            : null;

        if (insrtComp !== null) {
          return insrtComp;
        }

        return aEtlCtxId - bEtlCtxId;
      });

      console.log(JSON.stringify({ openRequestStatuses }, null, 4));
      return openRequestStatuses;
    },
  },

  async stopped() {
    await this.stopNpmrdsDownloadService();
  },
};
