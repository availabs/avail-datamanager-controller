import { fork } from "child_process";
import { hostname } from "os";
import { existsSync } from "fs";
import { join } from "path";

import { Context } from "moleculer";

import _ from "lodash";

import { FSA } from "flux-standard-action";

import { createNpmrdsDataRangeDownloadRequest } from "../../../../tasks/avail-datasources-watcher/src/utils/NpmrdsDataDownloadNames";

const npmrdsDownloadServiceMainPath = join(
  __dirname,
  "../../../../tasks/avail-datasources-watcher/src/dataSources/RITIS/services/NpmrdsDownloadService/main"
);

export const serviceName = "dama/data_sources/npmrds/travel_times_download";

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

      const stillRunning =
        this._downloadServiceCP && this._downloadServiceCP.exitCode !== null;

      if (stillRunning) {
        // TODO: Check return value and handle problem cases
        this._downloadServiceCP.kill();
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

      const isRunning =
        this._downloadServiceCP && this._downloadServiceCP.exitCode === null;

      if (!isRunning) {
        let ready: (...args: any) => void;
        let failed: (err: Error) => void;

        const done = new Promise((resolve, reject) => {
          ready = resolve;
          failed = reject;
        });

        clearTimeout(this._idleShutdownTimer);

        this._downloadServiceCP = fork(npmrdsDownloadServiceMainPath)
          // @ts-ignore
          .on("spawn", () => {
            console.log("npmrdsDownloadService spawned");
          })
          // @ts-ignore
          .on("error", failed)
          .on("message", (event: FSA) => {
            console.log(
              JSON.stringify({ npmrdsDownloadServiceEvent: event }, null, 4)
            );
            // KEEPALIVE events will resetIdleShutdownTimer
            this.resetIdleShutdownTimer();

            if (event.type === "NpmrdsDownloadService:READY") {
              this._npmrds_data_date_extent = event.payload;
              ready();
              this.resetIdleShutdownTimer();
            } else {
              this.handleNpmrdsDownloadServiceEvent.bind(this);
            }
          });

        await done;
      }

      return this._downloadServiceCP;
    },

    async handleNpmrdsDownloadServiceEvent(event: FSA) {
      // @ts-ignore
      const { type, meta: { pgEnv, etl_context_id } = {} } = event;

      if (!(pgEnv && etl_context_id)) {
        return;
      }

      // TODO: FINAL events must trigger data_manager.views updates.
      if (/:FINAL$/.test(type)) {
        const opts = {
          meta: {
            pgEnv,
          },
        };

        // FIXME: Need to emit the DamaView info into the event_store
        const newEvent = {
          ...event,
          type: `${serviceName}:FINAL`,
        };

        return await this.broker.call(
          "dama_dispatcher.dispatch",
          newEvent,
          opts
        );
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
  },

  actions: {
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

      console.log(ctx.params);

      const etl_context_id = await ctx.call("dama_dispatcher.spawnDamaContext");

      const dama_controller_host = hostname();

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

      // NpmrdsDownloadService.queueNpmrdsDownloadRequest(req);
      await this.queueNpmrdsExportRequest(req);

      return req.name;
    },

    async getNpmrdsDataDateExtent() {
      // May need to (re)start the NpmrdsDownloadService to get/refresh the _npmrds_data_date_extent
      await this.getNpmrdsDownloadService();

      return this._npmrds_data_date_extent;
    },
  },

  async stopped() {
    await this.stopNpmrdsDownloadService();
  },
};
