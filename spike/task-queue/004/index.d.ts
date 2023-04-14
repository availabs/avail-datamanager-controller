import { ExecaChildProcess } from "execa";

export type DamaTaskDescriptor = {
  task_id: string;
  pid?: number | null;
  worker_path: string;
};

export type DamaTask = DamaTaskDescriptor & {
  child_process: ExecaChildProcess | null;
};
