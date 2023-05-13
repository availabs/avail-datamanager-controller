import { TaskQueue } from "./domain";

export const TaskQueueConfigs = {
  [TaskQueue.AGGREGATE_ETL]: {
    worker_options: {
      teamSize: 32,
      teamConcurrency: 32,
      teamRefill: true,
    },
  },

  // Request and download NPMRDS Export from RITIS.
  [TaskQueue.DOWNLOAD_EXPORT]: {
    worker_options: {
      teamSize: 1,
      teamConcurrency: 1,
    },
  },

  // Load NPMRDS travel times and TMC_Identification into the SQLite db.
  [TaskQueue.TRANSFORM_EXPORT]: {
    worker_options: {
      teamSize: 3,
      teamConcurrency: 3,
      teamRefill: true,
    },
  },
};
