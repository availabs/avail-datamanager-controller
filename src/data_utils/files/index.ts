import { chmodSync } from "fs";
import { chmod as chmodAsync } from "fs/promises";

export function makeFileReadOnlySync(fpath: string) {
  chmodSync(fpath, 0o444);
}

export function makeFileReadOnlyAsync(fpath: string) {
  return chmodAsync(fpath, 0o444);
}
