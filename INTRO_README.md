# AVAIL DataManager Controller

## ETL Task Automation

The DataManager Controller runs ETL Tasks.

Simple Tasks can be run with a
[CLI interface](https://github.com/availabs/NPMRDS_Database/blob/master/src/transcom/transcom_events_aggregate_etl/run).

More complex ETL Tasks may be decomposed into many SubTasks that require coordination.

The Controller uses [Moleculer](#Moleculer) to compose and execute Tasks.

The DataManager Controller is designed to assimilate legacy ETL code.

## ETL Contexts

The state of an ETL Task is stored in a PostgreSQL database.
Each Task is assigned an ETL Context.
The ETL Contexts are conceptually modeled after Redux store slices.
Contexts are implemented using [EventSourcing].
EventStore events are [FluxStandardActions](https://github.com/redux-utilities/flux-standard-action).

```sql
dama_dev_1=# \d etl_context
                            Table "_data_manager_admin.etl_context"
   Column   |  Type   | Collation | Nullable |                     Default
------------+---------+-----------+----------+-------------------------------------------------
 context_id | integer |           | not null | nextval('etl_context_context_id_seq'::regclass)
 parent_id  | integer |           |          |

dama_dev_1=# \d event_store_prototype
                             Table "_data_manager_admin.event_store_prototype"
     Column     |  Type   | Collation | Nullable |                         Default
----------------+---------+-----------+----------+---------------------------------------------------------
 event_id       | integer |           | not null | nextval('event_store_prototype_event_id_seq'::regclass)
 etl_context_id | integer |           | not null |
 type           | text    |           | not null |
 payload        | jsonb   |           |          |
 meta           | jsonb   |           |          |
 error          | boolean |           |          |
Indexes:
    "event_store_prototype_pkey" PRIMARY KEY, btree (event_id)
```

## ETL Task State

The DataManager Controller server routes SHOULD be stateless.
A Task SHOULD rebuild it state from the DB EventStore.

## Resources

### [Moleculer](https://moleculer.services/)

-   [Services](https://moleculer.services/docs/0.14/services.html)
-   [Actions](https://moleculer.services/docs/0.14/actions.html)
-   [Context](https://moleculer.services/docs/0.14/context.html)
-   [API Gateway](https://moleculer.services/docs/0.14/moleculer-web.html)

### [EventSourcing](https://learn.microsoft.com/en-us/azure/architecture/patterns/event-sourcing)

-   [Greg Young GOTO 2014 Talk](https://www.youtube.com/watch?v=8JKjvY4etTY)
