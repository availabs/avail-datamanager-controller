# Data Manager Meeting 2023-04-03

## Big Changes

* DamaController core modules broken out from Moleculer.
  * Much more [SOLID](https://en.wikipedia.org/wiki/SOLID)
    * Core business logic is oblivious to whether it is running behind
      * Moleculer
      * A plain Express server
      * A CLI

* DamaTasks

  * pg-boss job queue allows queueing, retries, concurrency configuration.

  * DamaTasks can run
    * in the main DamaController process
    * in [detached](https://nodejs.org/dist/latest-v18.x/docs/api/child_process.html#optionsdetached)
      Node.js processes
      * started from the DamaController process
      * started from the command line
        * debuggable using the [Chrome Debugger](https://nodejs.org/en/docs/guides/debugging-getting-started)

### DamaTaskWorker Interface

Simple DamaTaskWorker
[example](https://github.com/availabs/avail-data-manager-controller/blob/dev-task-queue-integration/src/data_manager/tasks/examples/simple_foo_bar/worker.ts).

* DamaTaskWorkers are implemented in files that export a main function.

  * main function can be sync or async
    * `export default main function(initial_event)`
    * `export default async main function(initial_event)`

  * main function MAY take the EtlContext :INITIAL event as the sole optional parameter.

  * main function MAY dispatch its :FINAL event, or return it or its `payload` object.

  * worker file can be vanilla JavaScript or TypeScript

### DamaEtlContext

**ALL DamaTasks MUST have an EtlContext.**

ALL EtlContexts have a pg\_env and an etl_context_id.
All DamaController core module methods that interact with the database
MUST know the pg_env for a DamaTask. Many also need to know the etl_context_id.

Because EtlContexts are essential to ALL DamaTasks, and
to avoid needing to pass the EtlContext data throughout the entire DamaTask's call tree,
the DamaController implements contexts similar to as in
[React](https://react.dev/learn/passing-data-deeply-with-context) and
[Moleculer](https://moleculer.services/docs/0.14/context.html).

Using DamaControllerContexts, DamaController core module methods can get the
EtlContext metadata from the context, thereby allowing for much simpler
and ergonomic code when implementing DamaTasks.

Using DamaControllerContexts are optional, but highly recommended for they
enable developers to write DamaTask code within a simpler mental model, oblivious to

* the many-to-many relationship between DamaTasks and Databases
* the one-to-many relationship between DamaTask code and concurrently running DamaTask instances
