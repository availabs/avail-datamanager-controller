--  DROP VIEW IF EXISTS data_manager.dama_task_queue ;

CREATE OR REPLACE VIEW data_manager.dama_task_queue
  AS
    SELECT
        a.etl_context_id,
        a.parent_context_id,

        a.source_id,

        a.etl_task_id,
        a.etl_status,

        a.initial_event_id,
        a.latest_event_id,

        a._created_timestamp,
        a._modified_timestamp,
        b.id              AS task_id,
        b.name            AS task_queue_name,
        b.priority        AS task_priority,
        b.data            AS task_data,
        b.state           AS task_state,
        b.retrylimit      AS task_retry_limit,
        b.retrycount      AS task_retry_count,
        b.retrydelay      AS task_retry_delay,
        b.retrybackoff    AS task_retry_backoff,
        b.startafter      AS task_start_after,
        b.startedon       AS task_started_on,
        b.singletonkey    AS task_singleton_key,
        b.singletonon     AS task_singleton_on,
        b.expirein        AS task_expire_in,
        b.createdon       AS task_created_on,
        b.completedon     AS task_completed_on,
        b.keepuntil       AS task_keep_until,
        b.on_complete     AS task_on_complete,
        b.output          AS task_output,
        b.archivedon      AS task_archived_on

      FROM data_manager.etl_contexts AS a
        LEFT OUTER JOIN (
            SELECT
                id,
                name,
                priority,
                data,
                state,
                retryLimit,
                retryCount,
                retryDelay,
                retryBackoff,
                startAfter,
                startedOn,
                singletonKey,
                singletonOn,
                expireIn,
                createdOn,
                completedOn,
                keepUntil,
                on_complete,
                output,
                NULL AS archivedon
              FROM pgboss.job
            UNION
            SELECT
                id,
                name,
                priority,
                data,
                state,
                retryLimit,
                retryCount,
                retryDelay,
                retryBackoff,
                startAfter,
                startedOn,
                singletonKey,
                singletonOn,
                expireIn,
                createdOn,
                completedOn,
                keepUntil,
                on_complete,
                output,
                archivedon
              FROM pgboss.archive
          ) AS b
            ON ( a.etl_context_id = (b.data->>'etl_context_id')::INTEGER )
;
