import { spawn, ChildProcess } from "child_process";

import { mkdirSync } from "fs";

import {
  writeFile as writeFileAsync,
  readdir as readdirAsync,
} from "fs/promises";

import { join } from "path";

import chokidar from "chokidar";
import fetch from "node-fetch";
import _ from "lodash";

import mbtilesDir from "../../constants/mbtilesDir";

import serviceName from "./constants/serviceName";

type LocalVariables = {
  tileserverProcess: ChildProcess | null;
  tileserverConfig: {
    config: string;
    port: number;
    verbose: boolean;
    "no-cors": boolean;
    public_url: string;
  };
};

const configDir = join(__dirname, "/config");

mkdirSync(configDir, { recursive: true });

const tileserverConfigPath = join(configDir, "auto-generated-config.json");

const tileserverConfigFileBase = {
  options: {
    paths: {
      root: "",
    },
    formatQuality: {
      jpeg: 80,
      webp: 90,
    },
    maxScaleFactor: 3,
    maxSize: 2048,
    pbfAlias: "pbf",
    serveAllFonts: false,
    serveAllStyles: false,
    serveStaticMaps: true,
    tileMargin: 0,
  },
  styles: {
    basic: {
      style: "basic.json",
      tilejson: {
        type: "overlay",
        bounds: [8.44806, 47.32023, 8.62537, 47.43468],
      },
    },
  },
  data: {},
};

export default {
  name: serviceName,

  methods: {
    async initializeTileserverConfigFile() {
      const configPath = this.__local__.tileserverConfig.config;

      const config = _.cloneDeep(tileserverConfigFileBase);

      const mbtiles = await readdirAsync(mbtilesDir);

      config.data = mbtiles.reduce((acc, mbtile) => {
        const layerName = mbtile.replace(/\.mbtiles/, "");

        acc[layerName] = { mbtiles: join(mbtilesDir, mbtile) };

        return acc;
      }, {});

      await writeFileAsync(configPath, JSON.stringify(config, null, 4));
    },

    async restartTileServer() {
      await this.initializeTileserverConfigFile();

      this.__local__.tileserverProcess.kill("SIGHUP");

      await this.actions.getServerHealthStatus();
    },
  },

  actions: {
    async getTileServerUrl() {
      return this.__local__.tileserverConfig.public_url;
    },

    async getServerHealthStatus() {
      const {
        __local__: {
          tileserverConfig: { port },
        },
      } = this;

      const url = `http://127.0.0.1:${port}/health`;

      let retries = 0;
      while (retries < 10) {
        const code = await new Promise((resolve) =>
          setTimeout(async () => {
            try {
              const { status } = await fetch(url);

              return resolve(status);
            } catch (err) {
              resolve(404);
            }
          }, 1000)
        );

        if (code === 200) {
          return true;
        }

        ++retries;
      }

      throw new Error(
        "Staged Geospatial Dataset server failed health check. Is it running?"
      );
    },
  },

  created() {
    // @ts-ignore
    const port = +process.env.TILESERVER_PORT;

    if (!Number.isFinite(port)) {
      throw new Error(
        "Must set the TILESERVER_PORT in the project's .env file."
      );
    }

    // Because we may proxy
    const public_url =
      process.env.TILESERVER_PUBLIC_URL || `http://127.0.0.1:${port}`;

    const tileserverConfig = {
      config: tileserverConfigPath,
      port,
      verbose: true,
      // "no-cors": false,
      public_url,
    };

    this.__local__ = <LocalVariables>{
      tileserverConfig,
      tileserverProcess: null,
    };
  },

  async started() {
    const tileserverBinPath = join(
      __dirname,
      "../../../lib/tileserver-gl-light/src/main.js"
    );

    const tileserverArgs = Object.entries(
      this.__local__.tileserverConfig
    ).reduce((acc: string[], [k, v]: [string, any]) => {
      if (typeof v === "boolean" && v) {
        acc.push(`--${k}`);
      } else {
        acc.push(`--${k}`);
        acc.push(`${v}`);
      }

      return acc;
    }, []);

    await this.initializeTileserverConfigFile();

    console.log("spawning tileserver with the following configuration");
    console.log(JSON.stringify(this.__local__.tileserverConfig, null, 4));

    this.__local__.tileserverProcess = spawn(
      tileserverBinPath,
      tileserverArgs,
      {
        stdio: "inherit",
        env: {
          port: this.__local__.tileserverConfig.port,
        },
      }
    );

    this.__local__.tileserverProcess.on("error", (err: Error) => {
      console.error(
        "The tileserver child process emitted the following error:"
      );
      console.error(err);
    });

    this.__local__.tileserverProcess.on("spawn", () => {
      console.log("tileserver spawned");

      chokidar
        .watch(mbtilesDir, { ignoreInitial: true })
        .on("add", this.restartTileServer.bind(this))
        .on("unlink", this.restartTileServer.bind(this));
    });

    this.__local__.tileserverProcess.on(
      "exit",
      (code: number, signal: string) => {
        console.log(`tileserver exited with code ${code} by signal ${signal}`);
      }
    );

    const killTileserver = () => {
      if (this.__local__.tileserverProcess.exitCode === null) {
        this.__local__.tileserverProcess.kill();
      }
    };

    // SEE:
    //    https://nodejs.org/api/process.html#warning-using-uncaughtexception-correctly
    //    https://stackoverflow.com/a/14032965/3970755
    process.on("exit", killTileserver);

    process.on("uncaughtException", (err) => {
      killTileserver();
      throw err;
    });
  },

  async stopped() {
    try {
      this.__local__.tileserverProcess.kill("SIGTERM");
    } catch (err) {
      // ignore
    }
  },
};
