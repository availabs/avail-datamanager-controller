//  To run, from DamaController root:
//    ./node_modules/.bin/ts-node ./node_modules/.bin/ts-node spike/task-queue/005/monitorTasks.ts

import TasksController from "./TasksController";

const PG_ENV = "dama_dev_1";

async function main() {
  const tc = new TasksController(PG_ENV);

  await tc.ready;

  while (true) {
    const running_tasks = await tc.getDamaTasksRunningOnHost();

    const avail_task_environs = running_tasks.map(({ pid, environ }) => ({
      pid,
      ...environ,
    }));

    console.table(avail_task_environs);

    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
}

main();
