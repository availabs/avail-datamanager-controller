import { readFileSync } from "fs";

import { DamaTaskDescriptor } from "../index.d";

//  From https://man7.org/linux/man-pages/man5/proc.5.html
//    /proc/[pid]/environ
//        This file contains the initial environment that was set
//        when the currently executing program was started via
//        execve(2).  The entries are separated by null bytes
//        ('\0'), and there may be a null byte at the end.
export function getProcessDamaTaskId(pid: number) {
  try {
    const fpath = `/proc/${pid}/environ`;

    const envs = readFileSync(fpath, { encoding: "utf8" });

    const dama_task_id = envs
      .split("\0")
      .find((envLine) => /^AVAIL_DAMA_TASK_ID/.test(envLine))
      ?.replace(/.*=/, "");

    return dama_task_id || null;
  } catch (err) {
    return null;
  }
}

export function getDamaTaskIsRunning(dama_task_descr: DamaTaskDescriptor) {
  const { task_id, pid } = dama_task_descr;

  const proc_task_id = getProcessDamaTaskId(pid);

  const is_running = task_id === proc_task_id;

  return is_running;
}

// https://nodejs.org/docs/latest-v14.x/api/process.html#process_process_kill_pid_signal
export function killDamaTaskProcess(
  dama_task_descr: DamaTaskDescriptor,
  signal: string | number = "SIGTERM"
) {
  const is_running = getDamaTaskIsRunning(dama_task_descr);

  if (is_running) {
    process.kill(dama_task_descr.pid, signal);
  }
}
