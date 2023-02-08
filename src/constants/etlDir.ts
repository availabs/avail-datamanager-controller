import { mkdirSync } from "fs";
import { join } from "path";

const dir = join(__dirname, "../../tmp-etl");

mkdirSync(dir, { recursive: true });

export default dir;
