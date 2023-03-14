# AVAIL DamaTaskController Experiment 005

## TL;DR

1. [pg-boss](https://github.com/timgit/pg-boss) seems to work well for managing
   DamaTasks (while requiring a bit of a work-around)

2. The TaskManager and TaskController logic does seem to work well in providing:

    - Safe task retries (no duplicate tasks running due to TaskQueue "retries")
    - Detached tasks (running uncoupled from the TaskQueue process)
    - Task resuming (picking up where they left off)

### pg-boss' design and AVAIL's requirements

#### DamaTasks MUST run in [detached](https://nodejs.org/docs/latest-v14.x/api/child_process.html#child_process_options_detached) processes.

Why?

1. AVAIL will need to restart that DamaController process to deploy updates.
1. The TaskController will need to be a Service in the DamaController.
1. Some DamaTasks can take multiple days to complete (conflation, congestion, ...).

If all task processes are attached to the DamaController process, they will
need to be killed to restart the DamaController.

On the otherhand, if we must wait until all long-running tasks complete before
deploying new DamaController code, delivery of new DamaSources or DamaViews
must be scheduled in between long-running Tasks.

If DamaTask processes run independently of the DamaController process, the
DamaController can be restarted without killing the task processes.

Therefore, to allow long-running tasks to continue across DamaController
restarts, we use [execa](https://github.com/sindresorhus/execa) to run tasks in
detached processes.

#### pg-boss does not provide a public interface for tasks to self-report their status

The public interface that pg-boss provides to keep track of a task's status
requires the process in which pg-boss started the task to outlive the task.

The problem this design creates is that out-of-the-box pg-boss tasks are not
robust across pg-boss process restarts. If we restart the DamaController to
deploy new code, pg-boss handler functions will lose track of the tasks they
are monitoring and assume they failed/expired and require a restart.

More specifically, pg-boss' public interface uses an async
[handler](https://github.com/timgit/pg-boss/blob/HEAD/docs/readme.md#workname--options-handler)
function to keep track of task progress. If the handler successfully completes,
the task is marked as complete. If the handler throws an error, the task is
marked as failed. If the handler exceeds the configured timeout, the job is
marked as expired.

There is no public interface for tasks to self-report their status.
Their handler reports their status for them.

See:

-   [pg-boss docs](https://github.com/timgit/pg-boss/blob/HEAD/docs/readme.md)
-   [Spawn child processes for each job out of one worker](https://github.com/timgit/pg-boss/issues/280)
-   [Failing jobs on server shutdown](https://github.com/timgit/pg-boss/issues/102)

##### pg-boss with detached processes work-around

We shall define a "duplicate task" as an instance of a task started while
another instance of that task is already running.

Duplicate tasks can happen when the TaskController process exits before the
running task process exits. As the pg-boss worker handler ends before the task
process, pg-boss loses track of the running task process. Consequently, pg-boss
will eventually try to "restart" the task. Unless we implement idempotency,
this "restart" would result in multiple instances of the same task running
concurrently and potentially corrupting each other's data.

We implement idempotency with the following two rules:

1. **ALL tasks MUST acquire a FOR UPDATE lock on their EtlContext's :INITIAL
   event.**
2. **Any task that fails to aquire the :INTIAL event lock MUST exit IMMEDIATELY
   with a [specific exit
   code](https://github.com/availabs/avail-data-manager-controller/blob/dev-task-queue-integration/spike/task-queue/experiments/005/types.ts#L42).**

Because duplicate task processes fail with a specific exit code, if a pg-boss
worker handler encounters this exit code while starting a worker process we
KNOW that the handler tried to create a duplicate task. This condition would
have occurred because the running task's pg-boss handler is no longer executing,
pg-boss lost the ability to monitor it, and thus tried to "restart" it. To
integrate the running task back into pg-boss' queue management logic, the
handler that tried to run the duplicate task MUST "adopt" the running task
and task responsibility for updating pg-boss on the state of the task.

More specifically, pg-boss worker handlers that tried to create duplicate tasks
MUST implement the following algorithm:

-   If a pg-boss handler encounters a task that fails because it was unable to
    aquire the :INITIAL event lock, this handler then becomes responsible for
    notifying pg-boss of the state of the DamaTask that does have the :INITIAL
    event lock.
    -   The pg-boss worker handler for the duplicate task repeatedly tries to
        aquire a lock on the :INITIAL event.
        -   While the DamaTask process with the :INITIAL event lock is running,
            the handler will fail to aquire the lock.
        -   Once the DamaTask process with the :INITAL event lock exits, the
            handler can aquire the lock.
    -   Once the :INITIAL event lock is aquired by the handler, the handler
        checks data_manager.etl_contexts.etl_status of the DamaTask.
        -   If the etl_status is 'DONE', the handler successfully returns,
            thereby notifying pg-boss that the task is "complete".
        -   If the etl_status is not 'DONE', the handler throws an Error,
            thereby notifying pg-boss that the task "failed".

This should ensure pg-boss' job_status for the DamaTask is accurate.

---

## Other Design Decisions

### The data_manager tables MUST be the single source of truth for DamaTask status

Even though pg-boss will be used to manage queuing DamaTasks, the state of any
given task MUST ALWAYS be determined using the data_manager.etl_contexts and
data_manager.event_store tables.

This will allow:

-   Safe retries of DamaTasks running in detached processes (idempotency)
-   Safe resume with snapshots (picking up where a task left off)
-   Simpler API support for requesting DamaTask queue status because the data
    remains in the larger Dama domain model.

### When to dispatch the :INITIAL event?

Currently, we dispatch the :INITIAL event when a DamaTask is queued, not when
it is actually started. Alternatively, we could let pg-boss hold the initial
configuration and dispatch the :INITIAL event in the pg-boss work handler
function once the task is ready to execute. The former approach is more in line
with the AVAIL's DataManager model--DamaTask requests are immediately
integrated into the data_manager schema tables and task state is
visible through those tables. The later approach might make it simpler to
leverage pg-boss' features to orchestrate tasks, however tasks would become
fully integrated into the DataManager only after they are started--not immediately
upon submittal.

One possible solution is to add a VIEW in the data_manager schema that
summarizes the pg-boss [job
table](https://github.com/timgit/pg-boss/blob/HEAD/docs/readme.md#job-table).

See [create_dama_pgboss_view.sql](./sql/create_dama_pgboss_view.sql).

NOTE: pg-boss jobIds are not available in the :INIITAL event if we dispatch
before pb-boss send. However, the pgboss.job.data and pgboss.archive.data JSONB
columns does contain the DamaTask's etl_context_id, so we could use that to
JOIN the data_manager.etl_contexts with the pg_boss.job/archive table to
provide the status of queued but unstarted tasks.

### More Idempotency

DamaTask requests could include a key that is added to :INITIAL events to
implement greated idempotency. An index could be added to the
data_manager.event_store table to speed up searching for that key to ensure
duplicate tasks are not queued by the UI.

### DamaController Services can register queues with the DamaTaskController

Different DamaTasks have different properties.
Some tasks will be short-running, others long-running.
Some tasks we will want to auto-retry, others perhaps not.

Allowing Services to register queues will allow services to configure queues
according to the nature of the Tasks they create.

---

### NOTES

#### Providing a VIEW that shows the :INITIAL event locked status of DamaTasks

**TL;DR:** Does not look possible unless we use advisory locks instead of the
current row-level lock on the EtlContext's :INITIAL event.

In the data_manager.dama_task_queue VIEW, it would be nice to have an
initial_event_locked column.

Unable to use pg_catalog.pg_locks to query whether rows locked because

> Although tuples are a lockable type of object, information about row-level
> locks is stored on disk, not in memory, and therefore row-level locks
> normally do not appear in this view. If a process is waiting for a row-level
> lock, it will usually appear in the view as waiting for the permanent
> transaction ID of the current holder of that row lock.
> --[pg_locks](https://www.postgresql.org/docs/current/view-pg-locks.html)

See also: https://engineering.nordeus.com/postgres-locking-revealed/#monitoring-locks

How we would have used it.

```sql
        -- https://www.postgresql.org/docs/current/view-pg-locks.html
        INNER JOIN pg_catalog.pg_database AS d
          ON  ( d.datname = current_database() )
        -- https://www.postgresql.org/docs/current/view-pg-locks.html
        INNER JOIN pg_catalog.pg_locks AS e
          ON (
            ( d.oid = e.database )
            AND
            ( 'data_manager.event_store'::regclass::oid = d.relation )
            AND
            (
          )
```

##### Using the [pgrowlocks](https://www.postgresql.org/docs/current/pgrowlocks.html) module

This would work but it would require full table scans of the
data_manager.event_store table, which will likely get very large.
This would very likely result in a considerable performance issue.

To use SKIP LOCKED in this VIEW, we would have to try to aquire a lock.
Even the weakest lock would block the DamaTasks from aquiring a lock.
If the VIEW was used in a long-running transaction, all tasks would be blocked.

-   https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE
-   https://www.postgresql.org/docs/current/explicit-locking.html#LOCKING-ROWS

##### Using Postges' [Advisory Locks](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS)

> Like all locks in PostgreSQL, a complete list of advisory locks currently
> held by any session can be found in the pg_locks system view.

Because advisory locks do show up in the pg_catalog.pg_locks table,
we could use them to ensure only one instance of a task runs at a time
by aquiring an advisory lock in the TaskManager on startup.
We could then use the pg_catalog.pg_locks table to determine if any
process is holding onto a an advisory lock for the EtlContextId.

This would enable the view to work but would require care to ensure
we don't have id collisions if we use advisory locks elsewhere.

-   https://www.postgresql.org/docs/9.1/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS
-   https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS
-   https://vladmihalcea.com/how-do-postgresql-advisory-locks-work/
-   https://stackoverflow.com/questions/44381090/advisory-locks-scope-in-postgresql
-   https://stackoverflow.com/a/23850540

##### SAVEPOINTs won't work

https://www.postgresql.org/message-id/B0E6F561-656E-4B3B-9615-D0F7A80DF027%40sitening.com
