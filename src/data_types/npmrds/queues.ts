import { TaskQueue } from "./domain";

export const TaskQueueConfigs = {
  // NOTE: Increasing concurrency can lead to "sorry, too many clients already" node-pg error.
  [TaskQueue.AGGREGATE_ETL]: {
    worker_options: {
      teamSize: 5,
      teamConcurrency: 5,
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
