//  To run, from DamaController root:
//    ./node_modules/.bin/ts-node ./node_modules/.bin/ts-node spike/task-queue/005/startTasks.ts

import { join } from "path";

import TasksController from "./TasksController";

const PG_ENV = "dama_dev_1";

const NUM_TASKS = +process.argv[2] || 0;

const worker_path = join(__dirname, "./worker.js");

async function main() {
  console.log("START");
  const tc = new TasksController(PG_ENV);

  await tc.ready;

  await tc.startTaskQueue();

  const dama_task_descr = {
    initial_event: {
      type: ":INITIAL",
      payload: { delay: 1000, msg: "Hello, World!." },
    },
    worker_path,
  };

  for (let i = 0; i < NUM_TASKS; ++i) {
    const { etl_context_id } = await tc.queueDamaTask(dama_task_descr);

    console.log("DamaTask queued with etl_context_id =", etl_context_id);
  }
}

main();
