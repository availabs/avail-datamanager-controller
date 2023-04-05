// https://gist.github.com/pguillory/729616?permalink_comment_id=1946644#gistcomment-1946644

import { WriteStream } from "fs";

type StdOut = typeof process.stdout;
type StdErr = typeof process.stderr;

export function echoWriteStream(
  std: WriteStream | StdOut | StdErr,
  logStream: WriteStream
) {
  var old_write = std.write.bind(std);

  const new_write = function (...args: any[]) {
    old_write(...args);
    logStream.write.call(logStream, ...args);
  };

  std.write = new_write.bind(std);

  // disable echoing the output
  return () => (std.write = old_write);
}

export const echoStdoutToWriteStream = (logStream: WriteStream) =>
  echoWriteStream(process.stdout, logStream);

export const echoStderrToWriteStream = (logStream: WriteStream) =>
  echoWriteStream(process.stderr, logStream);

export const echoConsoleToWriteStream = (logStream: WriteStream) => {
  const disableStdoutEcho = echoStderrToWriteStream(logStream);
  const disableStderrEcho = echoStderrToWriteStream(logStream);

  return () => {
    disableStdoutEcho();
    disableStderrEcho();
  };
};
