import { existsSync, writeFileSync, readFileSync } from "fs";
import { join } from "path";
import dedent from "dedent";

import { v4 as uuid } from "uuid";

const host_id_fpath = join(__dirname, "../../config/dama_host_id");

let host_id: string;

if (!existsSync(host_id_fpath)) {
  host_id = uuid();

  const d = dedent(`
    # This file is auto-generated. Do not modify or delete.
    ${host_id}
  `);

  writeFileSync(host_id_fpath, d);
} else {
  host_id = readFileSync(host_id_fpath, { encoding: "utf8" })
    .split(/\n/)
    .filter((line) => !/^#/.test(line))[0];
}

export default host_id;
