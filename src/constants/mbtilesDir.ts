import { join } from "path";
import { mkdirSync } from "fs";

const mbtilesDir = join(__dirname, "../../dama_mbtiles");

mkdirSync(mbtilesDir, { recursive: true });

export default mbtilesDir;
