import { createWriteStream } from "fs";
import { join } from "path";

import { echoStdoutToLogStream } from "./logging";

const logStream = createWriteStream(join(__dirname, "./test.log"));

logStream.on("ready", () => {
  console.log("foo");

  const off = echoStdoutToLogStream(logStream);

  for (let i = 0; i < 100; ++i) {
    console.log("->", i);
  }

  // logStream.close();
  off();

  console.log("bar");
});
