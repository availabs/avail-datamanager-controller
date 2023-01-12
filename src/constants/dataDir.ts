import { mkdirSync } from "fs";
import { join } from "path";

const dir =
  process.env.DAMA_SERVER_FILESTORAGE_PATH ||
  join(__dirname, "../../dama-files");

mkdirSync(dir, { recursive: true });

export default dir;
