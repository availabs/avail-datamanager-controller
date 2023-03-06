# DamaController Tasks Experiment 003

## Change Log

-   003: TaskManager Module
-   002: Restartable Workflows.
-   001: Identifying DamaTasks

## How to demo:

Three terminals:

1. To watch the log file, from this directory:

```sh
$ touch log
$ tail -f log
```

2. To watch the database modifications, from this directory

```sh
$ watch 'sqlite3 -header -column db.sqlite3 "SELECT * FROM events order by task_id, event_id"'
```

3. To run the code, from the repository's root:

NOTE: Repeat the following command while killing the process using CTRL-C. You
will see that the processing continues despite the parent process terminating.
Additionally, restarting a parent process will safely restart any Tasks that
ended due to errors or self-termination.

```sh
$ ./node_modules/.bin/ts-node spike/task-queue/003/index.ts
```
