import { hostname } from "os";
import { join } from "path";
import { mkdirSync } from "fs";


// Host Name---------------------------------------
export const damaHost = hostname();
//-------------------------------------------------


// Dir for External Library Dependencies-----------
export const libDir = join(__dirname, "../../lib");
//-------------------------------------------------


// tmp dir for ETL data work-----------------------
export const etlDir = join(__dirname, "../../tmp-etl");
mkdirSync(etlDir, { recursive: true });

//-------------------------------------------------


// Root directory----------------------------------
export const rootDir = join(__dirname, "../..");
mkdirSync(rootDir, { recursive: true });

//-------------------------------------------------


// Dir for keeping processed data to serve---------
export const dataDir =
  process.env.DAMA_SERVER_FILESTORAGE_PATH ||
  join(__dirname, "../../dama-files");
mkdirSync(dataDir, { recursive: true });

//-------------------------------------------------

// Dir for mbtiles files for tileserver------------
export const mbtilesDir = join(__dirname, "../../dama_mbtiles");
mkdirSync(mbtilesDir, { recursive: true });

//-------------------------------------------------