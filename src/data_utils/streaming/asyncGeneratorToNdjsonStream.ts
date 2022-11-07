import { Readable } from "stream";

export default function asyncGeneratorToNdjsonStream(
  iter: AsyncGenerator<object>
) {
  async function* toNdjson() {
    for await (const feature of iter) {
      yield `${JSON.stringify(feature)}\n`;
    }
  }

  return Readable.from(toNdjson(), { objectMode: false });
}
