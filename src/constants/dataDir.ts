import { mkdirSync } from "fs";
import { join } from "path";

const dir = join(__dirname, "../../data");

mkdirSync(dir, { recursive: true });

export default dir;
